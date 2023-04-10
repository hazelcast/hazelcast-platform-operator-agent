# Hazelcast Platform Operator Agent #

<img align="right" src="https://hazelcast.com/brand-assets/files/hazelcast-stacked-flat-sm.png">

Platform Operator Agent enables users to utilize Hazelcast Platform's features easily in Kubernetes environments. 
The agent is implemented using [Go CDK](https://gocloud.dev/) for cloud providers support which allows the agent to become mostly cloud provider agnostic. Supported providers are: AWS, GCP and Azure.

The agent is used by Hazelcast Platform Operator for supporting multiple features. The features are: 

- [User Code Deployment](#user-code-deployment)
- [Restore](#restore)
- [Backup](#backup)

## User Code Deployment

There are two commands for user code deployment: `user-code-bucket` and `user-code-url`

### User Code from Buckets

Agent downloads `jar` files from a specified bucket and puts it under destined path. Learn more about `user-code-bucket` command using the `--help` argument.

### User Code from URLs

Agent downloads files from a specified URLs and puts them under destined path. Learn more about `user-code-url` command using the `--help` argument.

## Restore

Agent restores backup files stored as `.tar.gz` archives from specified bucket and puts the files under destined path. Learn more about `restore` command using the `--help` argument.

## Backup

Backup command starts an HTTP server for Backup related tasks. Learn more about `backup` command using the `--help` argument. It exposes the following endpoints:

- `POST /upload`: Agent starts an asynchronous backup process. It uploads the latest Hazelcast backup into specified bucket, arhiving the folder in the process. Returns an id of the backup process.
- `GET /upload/{id}`: Returns the status of the backup.
- `POST /upload/{id}/cancel`: Cancels the backup process.
- `DELETE /upload/{id}`: Deletes the backup process status.
- `GET /health`: Returns success if application is running.

## License

Please see the [LICENSE](LICENSE) file.
