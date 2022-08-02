# Hazelcast Platform Operator Agent #

<img align="right" src="https://hazelcast.com/brand-assets/files/hazelcast-stacked-flat-sm.png">

Platform Operator Agent enables users to utilize Hazelcast Platform's features easily in Kubernetes environments. It is used by Hazelcast Platform Operator for supporting multiple features. Supported features are:

- [Custom Class Download](#custom-class-download)
- [Restore](#restore)
- [Backup](#backup)

## Custom Class Download

Agent downloads `jar` files from a specified bucket and puts it under destined path. Learn more about `ccd` command using the `--help` argument.

## Restore

Agent restores backup files stored as `.tar.gz` archieves from specified bucket and puts in under destined path. Learn more about `restore` command using the `--help` argument.

## Backup

Backup command starts an HTTP server for Backup related tasks. Learn more about `backup` command using the `--help` argument. It exposes the following endpoints:

- `POST /upload`: Agent starts a asynchronous backup process. It uploads the latest Hazelcast backup into specified bucket, arhiving the folder in the process. Returns an id of the backup process.
- `GET /upload/{id}`: Returns the status of the backup.
- `DELETE /upload/{id}`: Cancels the backup process.
- `GET /health`: Returns success if application is running.

## License

Please see the [LICENSE](LICENSE) file.
