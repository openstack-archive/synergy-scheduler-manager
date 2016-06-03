# Packaging synergy-scheduler-manager

## Using docker

1. build the image

  ```shell
  cd packaging/docker/{ubuntu-14.04,centos7}
  docker build -t scheduler-builder-{centos7,ubuntu-14.04} .
  ```

2. run the image

  ```shell
  Docker run -i -v /path/to/synergy-scheduler-manager:/tmp/python-synergy-scheduler-manager scheduler-builder-{centos7,ubuntu-14.04}
  ```
  the deb/rpm will be output in `path/to/synergy-scheduler-manager/build`
