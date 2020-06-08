# tap-s3-parquet
## Installation
In your virtual environment of choice
```bash
pip install tap-s3-parquet
```

## Development

If you're working on tap-s3-parquet locally I highly recommend using flit to install it:
1. Activate your development virtualenv
2. Install flit: `pip install flit`
3. Keeping your virtualenv activated, install tap-s3-parquet via 
`cd /path/to/tap-s3-parquet/repo && flit install -s`

This serves as an alternative to setuptools console-scripts.

### TODO
1. Type stubs for singer :(