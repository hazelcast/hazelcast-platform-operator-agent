package util

const (
	BucketDataS3AccessKeyID        = "access-key-id"
	BucketDataS3SecretAccessKey    = "secret-access-key"
	BucketDataS3Region             = "region"
	BucketDataS3EnvAccessKeyID     = "AWS_ACCESS_KEY_ID"
	BucketDataS3EnvSecretAccessKey = "AWS_SECRET_ACCESS_KEY"
	BucketDataS3EnvRegion          = "AWS_REGION"

	BucketDataGCPCredentialFile    = "google-credentials-path"
	BucketDataGCPEnvCredentialFile = "GOOGLE_APPLICATION_CREDENTIALS"

	BucketDataAzureStorageAccount    = "storage-account"
	BucketDataAzureStorageKey        = "storage-key"
	BucketDataAzureEnvStorageAccount = "AZURE_STORAGE_ACCOUNT"
	BucketDataAzureEnvStorageKey     = "AZURE_STORAGE_KEY"

	AWS   = "s3"
	GCP   = "gs"
	AZURE = "azblob"
)
