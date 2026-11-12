import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

from progressbar import printProgressBar

ITEMS_PER_BATCH = 25
MAX_BATCH_ATTEMPTS = 10
MAX_BACKOFF_SECONDS = 30
MAX_WORKERS = 64

# Adaptive mode retries throttling errors with client-side rate limiting
BOTO_CONFIG = Config(retries={'max_attempts': 10, 'mode': 'adaptive'})


class CopyError(Exception):
    pass


def rename_field(name, fieldsToChange):
    """Return the attribute name after applying the rename mappings in order."""
    for old, new in fieldsToChange:
        if name == old:
            name = new
    return name


def check_key_schemas(client, source, target, fieldsToChange):
    """Abort early if the target table cannot accept the source items.

    Compares key attribute names, roles (HASH/RANGE) and types, with the
    rename mappings applied to the source key names. A target with a
    different key would either reject the items or let items overwrite each
    other while the copy reports success.
    """
    def key_schema(table_name, renames=()):
        table = client.describe_table(TableName=table_name)['Table']
        types = {
            definition['AttributeName']: definition['AttributeType']
            for definition in table.get('AttributeDefinitions', [])
        }
        return sorted(
            (key['KeyType'], rename_field(key['AttributeName'], renames), types.get(key['AttributeName']))
            for key in table['KeySchema']
        )

    source_keys = key_schema(source, fieldsToChange)
    target_keys = key_schema(target)
    if source_keys != target_keys:
        raise CopyError(
            "Key schema mismatch (key type, attribute name after renames, attribute type). Source %s: %s, target %s: %s"
            % (source, source_keys, target, target_keys)
        )


def write_batch(client, target, batch, on_written):
    """Write a batch, retrying anything DynamoDB returns as unprocessed.

    batch_write_item succeeds even when only part of the batch was written,
    so the response has to be checked and the remainder resubmitted.
    on_written is called with the number of items acknowledged by each
    response, so partial successes are counted even if the batch later fails.
    """
    pending = batch
    for attempt in range(MAX_BATCH_ATTEMPTS):
        response = client.batch_write_item(RequestItems={target: pending})
        unprocessed = response.get('UnprocessedItems', {}).get(target, [])
        on_written(len(pending) - len(unprocessed))
        pending = unprocessed
        if not pending:
            return
        time.sleep(min(MAX_BACKOFF_SECONDS, 0.1 * (2 ** attempt)))

    raise CopyError(
        "%d items still unprocessed after %d attempts"
        % (len(pending), MAX_BATCH_ATTEMPTS)
    )


def build_filter(field, value_from):
    """Build scan arguments keeping only items whose field is >= value_from.

    The field is compared as a string, e.g. an ISO 8601 date. Items that do
    not have the field never match. Returns an empty dict when no filter
    was requested.
    """
    if not field:
        return {}

    return {
        'FilterExpression': '#field >= :from',
        'ExpressionAttributeNames': {'#field': field},
        'ExpressionAttributeValues': {':from': {'S': value_from}},
    }


class Progress:
    """Thread-safe item counters and progress bar shared by all workers."""

    def __init__(self, estimated_total):
        self.estimated_total = estimated_total
        self.examined = 0
        self.scanned = 0
        self.written = 0
        self.lock = threading.Lock()
        self.render()

    def render(self, final=False):
        # The estimated count can be stale, keep the bar below 100% until the scan ends
        total = self.written if final else max(self.estimated_total, self.written + 1)
        printProgressBar(self.written, total, prefix='Progress:', suffix='Complete', length=50)

    def add_examined(self, count):
        with self.lock:
            self.examined += count

    def add_scanned(self, count):
        with self.lock:
            self.scanned += count

    def add_written(self, count):
        if not count:
            return
        with self.lock:
            self.written += count
            self.render()


def copy_segment(source, target, region, fieldsToChange, scan_filter, segment, total_segments, progress, stop):
    """Scan one segment of the source table and write its items to the target."""
    # Adaptive retry mode rate-limits per client, so each worker gets its own
    dynamo_client = boto3.client('dynamodb', region_name=region, config=BOTO_CONFIG)
    dynamo_target_client = boto3.client('dynamodb', region_name=region, config=BOTO_CONFIG)

    scan_args = {
        'TableName': source,
        'Select': 'ALL_ATTRIBUTES',
        'ReturnConsumedCapacity': 'NONE',
        'ConsistentRead': True,
        'PaginationConfig': {
            'PageSize': ITEMS_PER_BATCH,
        },
    }
    # The filter is applied by DynamoDB after reading, so pages can come back
    # with fewer items than PageSize, or none at all
    scan_args.update(scan_filter)
    if total_segments > 1:
        # Parallel scan: DynamoDB splits the table into disjoint segments
        scan_args['Segment'] = segment
        scan_args['TotalSegments'] = total_segments

    for page in dynamo_client.get_paginator('scan').paginate(**scan_args):
        if stop.is_set():
            return

        progress.add_examined(page.get('ScannedCount', len(page['Items'])))
        batch = []
        for item in page['Items']:
            for field in fieldsToChange:
                if field[0] in item:
                    item[field[1]] = item.pop(field[0])
            batch.append({
                'PutRequest': {
                    'Item': item
                }
            })

        # AWS has a minimum of 1 and a maximum of 25 items per batch
        if len(batch) == 0:
            continue

        progress.add_scanned(len(batch))
        try:
            write_batch(dynamo_target_client, target, batch, progress.add_written)
        except Exception as e:
            stop.set()
            with progress.lock:
                print()
                print("Failed to write batch to target table: %s" % e)
                print("Keys/items in the failed batch: %s" % batch)
            raise CopyError("Copy incomplete") from e


def migrate(source, target, region, fieldsToChange, workers=1, scan_filter=None):
    print("Copying contents of table %s to %s in region %s using %d worker(s)" % (source, target, region, workers))

    if len(fieldsToChange) > 0:
        fieldsToChange = [field.split(',') for field in fieldsToChange]
        for field in fieldsToChange:
            print("Changing field %s to %s" % (field[0], field[1]))

    scan_filter = scan_filter or {}
    if scan_filter:
        print("Filtering on %s >= %s" % (
            scan_filter['ExpressionAttributeNames']['#field'],
            scan_filter['ExpressionAttributeValues'][':from']['S'],
        ))

    dynamo_client = boto3.client('dynamodb', region_name=region, config=BOTO_CONFIG)

    check_key_schemas(dynamo_client, source, target, fieldsToChange)

    # Note that itemCount is an approximate value. The value in AWS is updated every 6 hours.
    itemCount = dynamo_client.describe_table(TableName=source)['Table'].get('ItemCount', 0)
    print("Source table has approximately %s items (as of AWS last update which could be up to 6 hours out of date)" % itemCount)
    if not itemCount:
        print("Using item count 100, the progress bar will not be accurate")
        itemCount = 100

    progress = Progress(itemCount)
    stop = threading.Event()
    error = None

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(copy_segment, source, target, region, fieldsToChange, scan_filter, segment, workers, progress, stop)
            for segment in range(workers)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                # Tell the other workers to stop after their current batch
                stop.set()
                error = error or e

    if error:
        print()
        print("Aborting. Scanned %d items, wrote %d items." % (progress.scanned, progress.written))
        if isinstance(error, CopyError):
            raise error
        raise CopyError("Copy incomplete: %s" % error) from error

    if progress.written:
        progress.render(final=True)
    else:
        print()
    if scan_filter:
        print("Filter matched %d of %d items examined." % (progress.scanned, progress.examined))
    print("Done. Scanned %d items, wrote %d items." % (progress.scanned, progress.written))


if __name__ == '__main__':
    parser = argparse.ArgumentParser("DynamoDB table copier using boto3")
    parser.add_argument("sourceTable", help="Source table name", type=str)
    parser.add_argument("targetTable", help="Target table name", type=str)
    parser.add_argument("--region", help="AWS region (default: eu-west-2)", type=str, nargs="?", default="eu-west-2")
    parser.add_argument("--fieldsToChange", help="Field name to change, and new name to use e.g. 'oldField,newField'", type=str, nargs='*', default=[])
    parser.add_argument("--workers", help="Number of parallel scan segments/threads (default: 1)", type=int, default=1)
    parser.add_argument("--filterField", help="Only copy items whose value in this top-level string field is >= --filterFrom", type=str)
    parser.add_argument("--filterFrom", help="Lower bound for --filterField, inclusive e.g. '2026-01-01T00:00:00Z'", type=str)
    args = parser.parse_args()
    if not 1 <= args.workers <= MAX_WORKERS:
        parser.error("--workers must be between 1 and %d" % MAX_WORKERS)
    for field in args.fieldsToChange:
        if len(field.split(',')) != 2 or not all(field.split(',')):
            parser.error("--fieldsToChange '%s' must be in the form 'oldField,newField'" % field)
    if bool(args.filterField) != (args.filterFrom is not None):
        parser.error("--filterField and --filterFrom must be used together")
    scan_filter = build_filter(args.filterField, args.filterFrom)
    try:
        migrate(args.sourceTable, args.targetTable, args.region, args.fieldsToChange, args.workers, scan_filter)
    except CopyError as e:
        print("ERROR: %s" % e, file=sys.stderr)
        sys.exit(1)
