import argparse
import sys
import time

import boto3
from botocore.config import Config

from progressbar import printProgressBar

ITEMS_PER_BATCH = 25
MAX_BATCH_ATTEMPTS = 10
MAX_BACKOFF_SECONDS = 30

# Adaptive mode retries throttling errors with client-side rate limiting
BOTO_CONFIG = Config(retries={'max_attempts': 10, 'mode': 'adaptive'})


class CopyError(Exception):
    pass


def check_key_schemas(client, source, target):
    """Abort early if the target table cannot accept the source items."""
    source_keys = client.describe_table(TableName=source)['Table']['KeySchema']
    target_keys = client.describe_table(TableName=target)['Table']['KeySchema']

    def normalise(schema):
        return sorted((key['KeyType'], key['AttributeName']) for key in schema)

    if normalise(source_keys) != normalise(target_keys):
        raise CopyError(
            "Key schema mismatch. Source %s: %s, target %s: %s"
            % (source, source_keys, target, target_keys)
        )


def write_batch(client, target, batch):
    """Write a batch, retrying anything DynamoDB returns as unprocessed.

    batch_write_item succeeds even when only part of the batch was written,
    so the response has to be checked and the remainder resubmitted.
    """
    pending = batch
    for attempt in range(MAX_BATCH_ATTEMPTS):
        response = client.batch_write_item(RequestItems={target: pending})
        pending = response.get('UnprocessedItems', {}).get(target, [])
        if not pending:
            return
        time.sleep(min(MAX_BACKOFF_SECONDS, 0.1 * (2 ** attempt)))

    raise CopyError(
        "%d items still unprocessed after %d attempts"
        % (len(pending), MAX_BATCH_ATTEMPTS)
    )


def migrate(source, target, region, fieldsToChange):
    print("Copying contents of table %s to %s in region %s" % (source, target, region))

    if len(fieldsToChange) > 0:
        fieldsToChange = [field.split(',') for field in fieldsToChange]
        for field in fieldsToChange:
            print("Changing field %s to %s" % (field[0], field[1]))

    dynamo_client = boto3.client('dynamodb', region_name=region, config=BOTO_CONFIG)
    dynamo_target_client = boto3.client('dynamodb', region_name=region, config=BOTO_CONFIG)

    if not fieldsToChange:
        # Renaming fields may legitimately change the key attribute names
        check_key_schemas(dynamo_client, source, target)

    dynamo_paginator = dynamo_client.get_paginator('scan')
    dynamo_response = dynamo_paginator.paginate(
        TableName=source,
        Select='ALL_ATTRIBUTES',
        ReturnConsumedCapacity='NONE',
        ConsistentRead=True,
        PaginationConfig={
            'PageSize': ITEMS_PER_BATCH,
        }
    )

    # Note that itemCount is an approximate value. The value in AWS is updated every 6 hours.
    itemCount = dynamo_client.describe_table(TableName=source)['Table'].get('ItemCount', 0)
    print("Source table has approximately %s items (as of AWS last update which could be up to 6 hours out of date)" % itemCount)
    if not itemCount:
        print("Using item count 100, the progress bar will not be accurate")
        itemCount = 100

    scanned = 0
    written = 0
    printProgressBar(0, itemCount, prefix='Progress:', suffix='Complete', length=50)
    for page in dynamo_response:
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

        scanned += len(batch)
        try:
            write_batch(dynamo_target_client, target, batch)
        except Exception as e:
            print()
            print("Failed to write batch to target table: %s" % e)
            print("Keys/items in the failed batch: %s" % batch)
            print("Aborting. Scanned %d items, wrote %d items." % (scanned, written))
            raise CopyError("Copy incomplete") from e

        written += len(batch)
        # The estimated count can be stale, keep the bar below 100% until the scan ends
        printProgressBar(written, max(itemCount, written + 1), prefix='Progress:', suffix='Complete', length=50)

    if written:
        printProgressBar(written, written, prefix='Progress:', suffix='Complete', length=50)
    else:
        print()
    print("Done. Scanned %d items, wrote %d items." % (scanned, written))


if __name__ == '__main__':
    parser = argparse.ArgumentParser("DynamoDB table copier using boto3")
    parser.add_argument("sourceTable", help="Source table name", type=str)
    parser.add_argument("targetTable", help="Target table name", type=str)
    parser.add_argument("--region", help="AWS region (default: eu-west-2)", type=str, nargs="?", default="eu-west-2")
    parser.add_argument("--fieldsToChange", help="Field name to change, and new name to use e.g. 'oldField,newField'", type=str, nargs='*', default=[])
    args = parser.parse_args()
    try:
        migrate(args.sourceTable, args.targetTable, args.region, args.fieldsToChange)
    except CopyError as e:
        print("ERROR: %s" % e, file=sys.stderr)
        sys.exit(1)
