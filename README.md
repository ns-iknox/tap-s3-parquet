# tap-s3-parquet [WIP]
## Installation
In your virtual environment of choice
```bash
pip install tap-s3-parquet
```

## Usage
```
Usage: tap-s3-parquet [OPTIONS]

  Singer tap to retrieve data from parquet files stored in S3

Options:
  --log-level [DEBUG|INFO|WARNING|ERROR|CRITICAL]
                                  [default: INFO]
  --discover                      Run tap in discovery mode
  --config TEXT                   Path to config file  [required]
  --state TEXT                    Path to state file
  --catalog TEXT                  Path to catalog file
  --help                          Show this message and exit.
```
If you're unfamiliar with how singer taps operate, these may help:
* https://github.com/singer-io/getting-started/tree/master/docs
* https://blog.panoply.io/etl-with-singer-a-tutorial


## Development

If you're working on tap-s3-parquet locally I highly recommend using flit to install it:
1. Activate your development virtualenv
2. Install flit: `pip install flit`
3. Keeping your virtualenv activated, install tap-s3-parquet via 
`cd /path/to/tap-s3-parquet/repo && flit install -s`

This serves as an alternative to setuptools console-scripts.
All AWS interactions are currently handled by [aws-data-wrangler](https://aws-data-wrangler.readthedocs.io/en/latest/)
