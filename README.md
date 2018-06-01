# Object Bench

## What is this?

Object Bench is a benchmarking tool to create, upload, download and delete objectstore buckets (containers) and objects (blobs) into a few public cloud stores (currently AWS, Azure, Google Cloud). At this time it supports uploading a directory (its constituent files being treated as multiple objects) to a single bucket.

## Usage

All credentials are read-off set OS environment variables. Please obtain the required variables from the cloud provider. To see which environment variables are needed please see the "__main__" block of the corresponding "*exercizer.py" code file in this repository.
