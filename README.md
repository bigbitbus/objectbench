# Object Bench

## What is this?

Object Bench is a benchmarking tool to create, upload, download and delete object store buckets (containers) and objects (blobs) into some public cloud stores (currently AWS, Azure, Google Cloud). At this time it supports uploading a directory (its constituent files being treated as multiple objects) to a single bucket.

## Usage

All credentials are read-off set OS environment variables. Please obtain the required variables from the cloud provider. To see which environment variables are needed please see the "__main__" block of the corresponding "*exercizer.py" code file in this repository.

Install the requirements (preferably into a python virtual environment):
```
pip install -r requirements.txt
```
After putting the credentials into environment variables (using e.g. the source statement below), run the benchmark. For example:

```
$ source {secure_folder}/credentials.sh
$ python awsexercizer.py
Upload objects
{'/tmp/smalldir/MAINTAINERS': (1.1708409786224365, 446794),
 '/tmp/smalldir/cpuinfo': (0.6111950874328613, 1748),
 '/tmp/smalldir/downloads/MAINTAINERS': (0.5888941287994385, 446794),
 '/tmp/smalldir/downloads/cpuinfo': (0.4392850399017334, 1748),
 '/tmp/smalldir/downloads/iomem': (0.23023605346679688, 1043),
 '/tmp/smalldir/downloads/meminfo': (0.4946460723876953, 1307),
 '/tmp/smalldir/iomem': (0.3935070037841797, 1043),
 '/tmp/smalldir/level1/iomem': (0.5325939655303955, 1043),
 '/tmp/smalldir/meminfo': (0.1788921356201172, 1307),
 '/tmp/smalldir/uploads/MAINTAINERS': (0.7265870571136475, 446794),
 '/tmp/smalldir/uploads/cpuinfo': (0.2738769054412842, 1748),
 '/tmp/smalldir/uploads/iomem': (0.22115206718444824, 1043),
 '/tmp/smalldir/uploads/meminfo': (0.4289848804473877, 1307)}
Download objects
{'/tmp/smalldir/MAINTAINERS': (17.39839506149292, 446794),
 '/tmp/smalldir/cpuinfo': (0.2229619026184082, 1748),
 '/tmp/smalldir/downloads/MAINTAINERS': (20.07686996459961, 446794),
 '/tmp/smalldir/downloads/cpuinfo': (0.5823531150817871, 1748),
 '/tmp/smalldir/downloads/iomem': (0.520056962966919, 1043),
 '/tmp/smalldir/downloads/meminfo': (0.22242498397827148, 1307),
 '/tmp/smalldir/iomem': (0.22393798828125, 1043),
 '/tmp/smalldir/level1/iomem': (0.21981501579284668, 1043),
 '/tmp/smalldir/meminfo': (0.6320850849151611, 1307),
 '/tmp/smalldir/uploads/MAINTAINERS': (48.98902893066406, 446794),
 '/tmp/smalldir/uploads/cpuinfo': (0.32395100593566895, 1748),
 '/tmp/smalldir/uploads/iomem': (0.7771360874176025, 1043),
 '/tmp/smalldir/uploads/meminfo': (1.2843818664550781, 1307),
 '_listObjectsInContainer': (0.17245697975158691,
                             'Container objects listing time')}
Delete buckets
{'blobtester': 2.95768404006958, 'operation': 'Deleted'}
(objbench) root@sachin-udtp:~/bbbcode/objbench#

```

In the above run each uploaded file/downloaded object has a tuple (time in seconds, bytes).