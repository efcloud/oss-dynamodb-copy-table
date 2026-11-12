# dynamodb-copy-table-python3

A simple Python scrip to copy data from one table to another in the same AWS Region
This was tested using `python 3.12` and `boto3@1.34.109`

---

### Requirements

This script assumes a table is already created in the destination account.

- Python 3.12
- boto3 >= 1.44.1 (`pip install -r requirements.txt`)

```
# Pyenv v3.12
brew install pyenv
pyenv install 3.12
echo "3.12" > ~/.pyenv/version
eval "$(pyenv init -)"
python --version
```

Setup `venv`:

```
# Setup venv
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Usage

A simple usage example:

```shell
$ python dynamodb-copy-table.py src_table dst_table
```

### Arguments:

`region`: the default region is `eu-west-2`

```shell
export AWS_PROFILE=

python dynamodb-copy-table.py src_table dst_table --region=cn-north-1
```

`workers`: number of parallel threads (default `1`, max `64`). Each worker scans its own
segment of the source table using a DynamoDB parallel scan. Only worth using on large tables;
more workers consume more read capacity on the source and write capacity on the target.

```shell
python dynamodb-copy-table.py src_table dst_table --workers=8
```

`filterField`, `filterFrom`: only copy items whose value in a top-level string field is
greater than or equal to `filterFrom`, e.g. everything from a given date onward.
Both must be given together. Items that do not have the field are not copied.

```shell
python dynamodb-copy-table.py src_table dst_table --filterField=createdAt --filterFrom=2026-01-01T00:00:00Z
```

Values are compared as text, so this only works when every value uses the same sortable
format (such as ISO 8601 in a single timezone). The filter is applied after DynamoDB reads each
item, so the whole table is still scanned and read capacity is consumed for every item.
The filter is evaluated on the source field name, before any `--fieldsToChange` rename.

### References

- [Import and Export DynamoDB Data using AWS Data Pipeline](http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-importexport-ddb.html)
