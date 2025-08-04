# Remblob

**Edit remote files locally with your favorite editor**

Remblob is a command-line tool that makes working with remote files effortless. Download, edit in your local editor, and automatically upload changes back - all in one seamless workflow.

## ✨ Features

- 🎯 **Edit any remote file locally** using your preferred editor (`$EDITOR`)
- 📊 **Parquet file support** - automatically converts to CSV for editing, preserves schema
- 🗜️ **Smart compression handling** - automatically compress/decompress gzip files
- ☁️ **Multiple storage backends** - AWS S3, local files, custom S3 endpoints
- 🔄 **Round-trip integrity** - preserves data types and structure
- 📝 **View-only mode** - inspect files without making changes

## 🚀 Quick Start

```bash
# Edit a JSON file from S3
remblob edit s3://my-bucket/config.json

# Edit a parquet file (automatically converts to CSV for editing)
remblob edit s3://data-bucket/analytics.parquet

# Edit with compression
remblob edit s3://logs/app.log.gz

# Save to different location
remblob edit local-file.json s3://backup-bucket/file.json.gz

# View without editing
remblob view s3://my-bucket/readonly.json
```

## 📋 Supported File Formats

| Format | Extension | Notes |
|--------|-----------|-------|
| **Parquet** | `.parquet` | Converted to CSV for editing, schema preserved |
| **JSON** | `.json` | Direct editing |
| **Text files** | `.txt`, `.log`, etc. | Direct editing |
| **Compressed** | `.gz` | Automatic compression/decompression |

### Parquet Files
When editing parquet files, remblob:
- Converts to CSV format for easy editing in any text editor
- Preserves the original schema and data types
- Handles type inference and validation on save
- Provides clear error messages for type conversion issues

```bash
# Edit parquet data as CSV
remblob edit s3://analytics/user_events.parquet
# Opens CSV in your editor, saves back as parquet with original schema
```

## 🛠 Installation

### macOS
```bash
brew install techiecaro/tap/remblob
```

### Linux/Windows
Download the latest release for your platform:
```bash
# Download and install latest release
curl -sf https://gobinaries.com/techiecaro/remblob | sh
```

Or download directly from [GitHub Releases](https://github.com/techiecaro/remblob/releases)

### Go Install
```bash
go install github.com/techiecaro/remblob@latest
```

### Build from Source
```bash
git clone https://github.com/techiecaro/remblob
cd remblob
go build -o remblob .
```

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `EDITOR` | Your preferred text editor <br>(Needs `Wait for the files to be closed before returning` option) | `vim`, `code --wait`, `nano` |
| `AWS_ENDPOINT` | Custom S3 endpoint URL | `https://s3.us-west-2.amazonaws.com` |
| `AWS_NO_SIGN_REQUEST` | Enable anonymous S3 access | `true` |

### AWS Authentication

Remblob uses standard AWS credential chain:

1. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
2. **AWS credentials file**: `~/.aws/credentials`
3. **IAM roles** (when running on EC2)
4. **AWS profiles**: Use `AWS_PROFILE=myprofile`

```bash
# Using environment variables
export AWS_ACCESS_KEY_ID=your-key-id
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-west-2
remblob edit s3://my-bucket/file.json

# Using AWS profiles
AWS_PROFILE=production remblob edit s3://prod-bucket/config.json

# Anonymous access (public buckets)
AWS_NO_SIGN_REQUEST=true remblob view s3://public-bucket/data.json
```

## 📖 Usage Examples

### Basic Operations
```bash
# Edit and save to same location
remblob edit s3://bucket/config.json

# Edit and save to different location  
remblob edit input.json s3://bucket/output.json

# View without editing
remblob view s3://bucket/readonly.json
```

### Working with Parquet Files
```bash
# Edit parquet file - opens as CSV in your editor
remblob edit s3://analytics/events.parquet

# Convert local CSV to parquet
remblob edit data.csv s3://bucket/data.parquet

# View parquet data as CSV
remblob view s3://warehouse/sales.parquet
```

### Compression Examples
```bash  
# Edit compressed file
remblob edit s3://logs/app.log.gz

# Compress while saving
remblob edit config.json s3://backup/config.json.gz

# Decompress while saving
remblob edit s3://data/file.json.gz local-file.json
```

### Custom S3 Endpoints
```bash
# MinIO or other S3-compatible storage
AWS_ENDPOINT=https://minio.example.com remblob edit s3://bucket/file.json

# DigitalOcean Spaces
AWS_ENDPOINT=https://nyc3.digitaloceanspaces.com remblob edit s3://space/file.json
```

## 🔧 Command Reference

```
Usage: remblob <command>

Commands:
  edit <source_path> [<destination_path>]
    Edit a remote file locally. If destination_path is omitted, 
    saves back to source_path.

  view <source_path>  
    Open a remote file in read-only mode.

Flags:
  -h, --help    Show help information
```

## 🔍 Troubleshooting

### Common Issues

**"cannot convert 'value' to type"**
- When editing parquet files, ensure data types match the original schema
- Check the error message for the specific row and column with issues

**"AWS authentication failed"**
- Verify your AWS credentials are configured correctly
- Check that your IAM user/role has S3 permissions for the bucket

**"Editor not found"**
- Set the `EDITOR` environment variable: `export EDITOR=vim`
- Or use full path: `export EDITOR=/usr/bin/nano`

**"Permission denied"**
- Ensure your AWS credentials have read/write access to the S3 bucket
- For public buckets, try: `AWS_NO_SIGN_REQUEST=true remblob view s3://bucket/file`

### Getting Help
- Run `remblob --help` for usage information
- Check [GitHub Issues](https://github.com/techiecaro/remblob/issues) for known problems
- Create a new issue for bugs or feature requests

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

[MIT](https://choosealicense.com/licenses/mit/)

---

**Made with ❤️ by [techiecaro](https://github.com/techiecaro)**
