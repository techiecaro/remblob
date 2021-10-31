# Remblob

Command line tool making working with remote files easier.

It lets you edit or view remote files in your local editor.


## Usage

```bash
Usage: remblob <command>

Edit remote files locally.

    Example executions:
    remblob edit s3://a-bucket/path/blob.json
    remblob edit blob.json s3://a-bucket/path/blob.json.gz
    remblob view s3://a-bucket/path/blob.json

Flags:
  -h, --help    Show context-sensitive help.

Commands:
  edit <source_path> [<destination_path>]
    Edits a remote blob and optionally stores it elsewhere.

  view <source_path>
    Views a remote blob.

```

## Installation

### macOS

```
brew install techiecaro/tap/remblob
```

### Linux

(WIP)

## Run Locally

Clone the project

```bash
git clone https://github.com/techiecaro/remblob
```

Go to the project directory

```bash
cd remblob
```

Install dependencies

```bash
go install
```

Start the server

```bash
remblob --help
```

## License

[MIT](https://choosealicense.com/licenses/mit/)