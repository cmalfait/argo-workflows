package oci

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"time"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	ociauth "github.com/oracle/oci-go-sdk/v65/common/auth"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/argoproj/argo-workflows/v3/errors"
	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	waitutil "github.com/argoproj/argo-workflows/v3/util/wait"
	"github.com/argoproj/argo-workflows/v3/workflow/artifacts/common"
)

type ArtifactDriver struct {
	Provider        string
	CompartmentOCID string
	TenancyOCID     string
	UserOCID        string
	Region          string
	Fingerprint     string
	PrivateKey      string
	Passphrase      string
	AccessKey       string
	SecretKey       string
}

var (
	_            common.ArtifactDriver = &ArtifactDriver{}
	defaultRetry                       = wait.Backoff{Duration: time.Second * 2, Factor: 2.0, Steps: 5, Jitter: 0.1, Cap: time.Minute * 10}
)

func (ociDriver *ArtifactDriver) initializeConfigurationProvider() (context.Context, objectstorage.ObjectStorageClient) {
	var configurationProvider ocicommon.ConfigurationProvider

	// We accept 3 types of auth: instance_principal, k8s_secret, raw
	// We default to instance principal
	switch ociDriver.Provider {
	case "instance_principal":
		configurationProvider = initializeInstancePrincipalProvider()
	case "k8s_secret":
		// Trim whitespace, couldn't get this to work otherwise
		access := strings.TrimSpace(ociDriver.AccessKey)
		configurationProvider = ocicommon.NewRawConfigurationProvider(ociDriver.TenancyOCID, ociDriver.UserOCID, ociDriver.Region, access, ociDriver.SecretKey, nil)
	case "raw":
		configurationProvider = ocicommon.NewRawConfigurationProvider(ociDriver.TenancyOCID, ociDriver.UserOCID, ociDriver.Region, ociDriver.Fingerprint, ociDriver.PrivateKey, nil)
	default:
		log.Infof("INFO: Provider not set, defaulting to instance_principal")
		configurationProvider = initializeInstancePrincipalProvider()
	}

	storageClient, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(configurationProvider)
	if err != nil {
		log.Errorf("ERROR: ObjectStorageClient error %s", err)
	}

	ctx := context.Background()
	return ctx, storageClient
}

// instance principal authentication
func initializeInstancePrincipalProvider() ocicommon.ConfigurationProvider {
	configurationProvider, err := ociauth.InstancePrincipalConfigurationProvider()
	if err != nil {
		log.Errorf("ERROR: In instance principal config provider %s", err)
	}
	return configurationProvider
}

func (ociDriver *ArtifactDriver) ListObjects(artifact *wfv1.Artifact) ([]string, error) {
	log.Info("DEBUG: ListObjects")
	var files []string
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			log.Infof("INFO: OCI List bucket: %s, key %s", artifact.OCI.Bucket, artifact.OCI.Key)
			ctx, storageClient := ociDriver.initializeConfigurationProvider()
			namespace := getNamespace(ctx, storageClient)
			files = listByPrefix(ctx, storageClient, namespace, artifact.OCI.Bucket, artifact.OCI.Key)
			return true, nil
		})
	return files, err
}

func listByPrefix(ctx context.Context, storageClient objectstorage.ObjectStorageClient, namespace string, bucketname string, prefix_files_name string) []string {
	var filelist []string
	request := objectstorage.ListObjectsRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketname,
		Prefix:        &prefix_files_name,
	}
	r, err := storageClient.ListObjects(ctx, request)
	fatalIfError(err)

	for _, v := range r.ListObjects.Objects {
		log.Infof("INFO: %v\n", *v.Name)
		filelist = append(filelist, *v.Name)
	}
	if filelist == nil {
		log.Infof("INFO: No files with prefix %s found in bucket %s to download", prefix_files_name, bucketname)
		return nil
	}
	log.Infof("INFO: Filelist: %v", filelist)
	return filelist
}

// Load download from OCI storage to a local path
func (ociDriver *ArtifactDriver) Load(inputArtifact *wfv1.Artifact, path string) error {
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			log.Infof("INFO: OCI Load path: %v, key %v", path, inputArtifact.OCI.Key)
			ctx, storageClient := ociDriver.initializeConfigurationProvider()
			namespace := getNamespace(ctx, storageClient)
			err := downloadObject(ctx, storageClient, namespace, inputArtifact.OCI.Bucket, inputArtifact.OCI.Key, path)
			if err != nil {
				log.Warnf("WARNING: Failed to download object from OCI: %v", err)
				return !isTransientOCIErr(err), err
			}
			return true, nil
		})

	return err
}

// downloadObject downloads an object from OCI storage
func downloadObject(ctx context.Context, storageClient objectstorage.ObjectStorageClient, namespace string, bucketname string, objectname string, path string) (err error) {
	request := objectstorage.GetObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketname,
		ObjectName:    &objectname,
	}
	r, err := storageClient.GetObject(ctx, request)
	fatalIfError(err)

	filedata, err := ioutil.ReadAll(r.Content)
	fatalIfError(err)

	f, err := os.Create(path)
	fatalIfError(err)
	defer f.Close()

	_, err = f.Write(filedata)
	fatalIfError(err)

	log.Infof("INFO: Downloaded file %v from bucket %v to %v", objectname, bucketname, path)
	return err
}

// OpenStream is currently not implemented, uses load instead
func (ociDriver *ArtifactDriver) OpenStream(artifact *wfv1.Artifact) (io.ReadCloser, error) {
	log.Info("DEBUG: OpenStream")
	// todo: this is a temporary implementation utilizing load until stream is implemented 
	return common.LoadToStream(artifact, ociDriver)
}

// Save stores an artifact in OCI storage.  IE: uploads a local file to an OCI bucket
func (ociDriver *ArtifactDriver) Save(path string, outputArtifact *wfv1.Artifact) error {
	log.Info("DEBUG: Save")
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			objectName := filepath.Clean(outputArtifact.OCI.Key)
			ctx, storageClient := ociDriver.initializeConfigurationProvider()
			namespace := getNamespace(ctx, storageClient)
			bucketName := outputArtifact.OCI.OCIBucket.Bucket

			err := putObject(ctx, storageClient, namespace, path, bucketName, objectName)
			if err != nil {
				return !isTransientOCIErr(err), err
			}

			return true, nil
		})
	return err
}

// putObject uploads to an OCI bucket
func putObject(ctx context.Context, storageClient objectstorage.ObjectStorageClient, namespace string, path string, bucketName string, objectName string) error {
	uploadManager := transfer.NewUploadManager()
	req := transfer.UploadFileRequest{
		UploadRequest: transfer.UploadRequest{
			NamespaceName:                       &namespace,
			BucketName:                          &bucketName,
			ObjectName:                          &objectName,
			PartSize:                            ocicommon.Int64(100 * 1024 * 1024),
			CallBack:                            callBack,
			ObjectStorageClient:                 &storageClient,
			EnableMultipartChecksumVerification: ocicommon.Bool(true),
		},
		FilePath: path,
	}

	// if you want to overwrite the default value, you can do it
	// as: transfer.UploadRequest.AllowMultipartUploads = common.Bool(false) // default is true
	// or: transfer.UploadRequest.AllowParrallelUploads = common.Bool(false) // default is true
	resp, err := uploadManager.UploadFile(ctx, req)

	if err != nil && resp.IsResumable() {
		resp, err = uploadManager.ResumeUploadFile(ctx, *resp.MultipartUploadResponse.UploadID)
		if err != nil {
			log.Errorf("ERROR: %v", resp)
		}
	}
	return err
}

// callBack get information about a multiplart upload
func callBack(multiPartUploadPart transfer.MultiPartUploadPart) {
	if nil == multiPartUploadPart.Err {
		log.Infof("INFO: Part: %d / %d is uploaded.", multiPartUploadPart.PartNum, multiPartUploadPart.TotalParts)
		log.Infof("INFO: and this part opcMD5(64BasedEncoding) is: %s.\n", *multiPartUploadPart.OpcMD5)
	}
}

// Delete an artifact from OCI
func (ociDriver *ArtifactDriver) Delete(artifact *wfv1.Artifact) error {
	log.Info("DEBUG: Delete")
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			ctx, storageClient := ociDriver.initializeConfigurationProvider()

			namespace := getNamespace(ctx, storageClient)
			bucketName := artifact.OCI.OCIBucket.Bucket
			objectName := artifact.OCI.Key

			err := deleteObject(ctx, storageClient, namespace, bucketName, objectName)
			if err != nil {
				log.Errorf("ERROR: Could not delete object: %v", err)
			}
			return true, nil
		})
	return err
}

// deleteObject deletes an object from OCI
func deleteObject(ctx context.Context, storageClient objectstorage.ObjectStorageClient, namespace string, bucketname string, objectname string) (err error) {
	request := objectstorage.DeleteObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketname,
		ObjectName:    &objectname,
	}
	_, err = storageClient.DeleteObject(ctx, request)
	fatalIfError(err)
	log.Infof("INFO: Deleted object %v", objectname)
	return
}

// getNamespace return our namespace string
func getNamespace(ctx context.Context, client objectstorage.ObjectStorageClient) string {
	request := objectstorage.GetNamespaceRequest{}
	namespace, err := client.GetNamespace(ctx, request)
	fatalIfError(err)

	return *namespace.Value
}

// IsDirectory not implemented yet
func (ociDriver *ArtifactDriver) IsDirectory(artifact *wfv1.Artifact) (bool, error) {
	log.Info("DEBUG: IsDirectory")
	log.Infof("IsDirectory currently not implemented for OCI")
	return false, errors.New(errors.CodeNotImplemented, "IsDirectory currently not implemented for OCI")
}

// inTransient checks if our error is retryable
// from https://docs.oracle.com/en-us/iaas/tools/python/2.88.2/api/retry.html
// and https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm
func isTransientOCIErr(err error) bool {
	log.Info("DEBUG: isTransientOCIErr")

	if serviceError, ok := ocicommon.IsServiceError(err); ok && serviceError.GetHTTPStatusCode() == 429 || (serviceError.GetHTTPStatusCode() >= 500 && serviceError.GetHTTPStatusCode() < 600) {
		log.Info("INFO: Retrying status code: ", serviceError.GetHTTPStatusCode())
		return true
	}
	return false
}

// fatalIfError logs fatal error 
func fatalIfError(err error) {
	if err != nil {
		log.Errorf("ERROR: %v", err.Error())
	}
}