# Packaging synergy-scheduler-manager

## Using docker

1. build the image

  ```shell
  cd packaging/docker/{ubuntu-16.04,centos7}
  docker build -t scheduler-builder-{centos7,ubuntu-16.04} .
  ```

2. run the image

  ```shell
  Docker run -i -v /path/to/synergy-scheduler-manager:/tmp/synergy scheduler-builder-{centos7,ubuntu-16.04}
  ```
  the deb/rpm will be output in `path/to/synergy-scheduler-manager/build`

  You can override the package version that will be set during the packaging
  process by adding `-e "PKG_VERSION=x.y.z"` to the above command line.
  Otherwise, the package version will be set to the latest git tag.
