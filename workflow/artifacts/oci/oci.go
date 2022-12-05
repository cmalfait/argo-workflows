package oci

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"time"

	"github.com/oracle/oci-go-sdk/v65/objectstorage/transfer"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	ociauth "github.com/oracle/oci-go-sdk/v65/common/auth"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"

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
	PrivateKey		string
	Passphrase		string
	AccessKey       string
	SecretKey       string
}

var (
	_            common.ArtifactDriver = &ArtifactDriver{}
	defaultRetry                       = wait.Backoff{Duration: time.Second * 2, Factor: 2.0, Steps: 5, Jitter: 0.1, Cap: time.Minute * 10}
)

func (ociDriver *ArtifactDriver) initializeConfigurationProvider() (context.Context, objectstorage.ObjectStorageClient) {
	var configurationProvider ocicommon.ConfigurationProvider

	// We accept 3 types of auth.  1. instance_principal, 2. k8s_secret, 3. raw
	// We default to instance principal
	switch ociDriver.Provider {
	case "instance_principal":
		configurationProvider = initializeInstancePrincipalProvider() 
	case "k8s_secret":
		// Trim whitespace
		access := strings.TrimSpace(ociDriver.AccessKey)
		configurationProvider = ocicommon.NewRawConfigurationProvider(ociDriver.TenancyOCID, ociDriver.UserOCID, ociDriver.Region, access, ociDriver.SecretKey, nil)
	case "raw":
		configurationProvider = ocicommon.NewRawConfigurationProvider(ociDriver.TenancyOCID, ociDriver.UserOCID, ociDriver.Region, ociDriver.Fingerprint, ociDriver.PrivateKey, nil)
	default:
		configurationProvider = initializeInstancePrincipalProvider() 
	}

	storageClient, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(configurationProvider)
	if err != nil {
		log.Errorf("ObjectStorageClient error %s", err)
	}

	ctx := context.Background()
	return ctx, storageClient
}

// instance principal authentication
func initializeInstancePrincipalProvider() ocicommon.ConfigurationProvider {
	configurationProvider, err := ociauth.InstancePrincipalConfigurationProvider()
	if err != nil {
		log.Errorf("Error in instance principal config provider %s", err)
	}
	return configurationProvider
}

func (ociDriver *ArtifactDriver) Load(inputArtifact *wfv1.Artifact, path string) error {
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			log.Infof("OCI Load path: %s, key %s", path, inputArtifact.OCI.Key)
			return true, nil
		})

	return err
}

func (ociDriver *ArtifactDriver) OpenStream(artifact *wfv1.Artifact) (io.ReadCloser, error) {
	log.Info("DEBUG: OpenStream")
	return common.LoadToStream(artifact, ociDriver)
}

func (ociDriver *ArtifactDriver) Save(path string, outputArtifact *wfv1.Artifact) error {
	log.Info("DEBUG: Save")
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			objectName := filepath.Clean(outputArtifact.OCI.Key)
			ctx, storageClient := ociDriver.initializeConfigurationProvider()
			namespace := getNamespace(ctx, storageClient)
			bucketName := outputArtifact.OCI.OCIBucket.Bucket

			err := putObject(ctx, storageClient, namespace, path, bucketName, objectName, nil)
			if err != nil {
				log.Errorf("Error putting object: %v", err)
			}

			return true, nil
		})
	return err
}

// upload a local object or dir to OCI storage
func putObject(ctx context.Context, storageClient objectstorage.ObjectStorageClient, namespace, path, bucketName, objectName string, metadata map[string]string) error {
	uploadManager := transfer.NewUploadManager()
	req := transfer.UploadFileRequest{
		UploadRequest: transfer.UploadRequest{
			NamespaceName:                       ocicommon.String(namespace),
			BucketName:                          ocicommon.String(bucketName),
			ObjectName:                          ocicommon.String(objectName),
			PartSize:                            ocicommon.Int64(128 * 1024 * 1024),
			CallBack:                            callBack,
			ObjectStorageClient:                 &storageClient,
			EnableMultipartChecksumVerification: ocicommon.Bool(true),
		},
		FilePath: path,
	}
		
	// if you want to overwrite default value, you can do it
	// as: transfer.UploadRequest.AllowMultipartUploads = common.Bool(false) // default is true
	// or: transfer.UploadRequest.AllowParrallelUploads = common.Bool(false) // default is true
	resp, err := uploadManager.UploadFile(ctx, req)

	if err != nil && resp.IsResumable() {
		resp, err = uploadManager.ResumeUploadFile(ctx, *resp.MultipartUploadResponse.UploadID)
		if err != nil {
			fmt.Println(resp)
		}
	}
	return err
}

func callBack(multiPartUploadPart transfer.MultiPartUploadPart) {
	if nil == multiPartUploadPart.Err {
		// Please refer this as the progress bar print content.
		fmt.Printf("Part: %d / %d is uploaded.\n", multiPartUploadPart.PartNum, multiPartUploadPart.TotalParts)
		fmt.Printf("One example of progress bar could be the above comment content.\n")
		// Please refer following fmt to get each part opc-md5 res.
		fmt.Printf("and this part opcMD5(64BasedEncoding) is: %s.\n", *multiPartUploadPart.OpcMD5 )
	}
}

// log fatal error
func fatalIfError(err error) {
	if err != nil {
		log.Errorf(err.Error())
	}
}

// return our namespace string
func getNamespace(ctx context.Context, client objectstorage.ObjectStorageClient) string {
	request := objectstorage.GetNamespaceRequest{}
	r, err := client.GetNamespace(ctx, request)
	fatalIfError(err)

	//namespace string
	return *r.Value
}

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
				log.Errorf("Error deleting object: %v", err)
			}
			return true, nil
		})
	return err
}

func deleteObject(ctx context.Context, c objectstorage.ObjectStorageClient, namespace, bucketname, objectname string) (err error) {
	request := objectstorage.DeleteObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketname,
		ObjectName:    &objectname,
	}
	_, err = c.DeleteObject(ctx, request)
	fatalIfError(err)
	fmt.Println("Deleted object ", objectname)
	return
}

func (ociDriver *ArtifactDriver) ListObjects(artifact *wfv1.Artifact) ([]string, error) {
	log.Info("DEBUG: ListObjects")
	var files []string
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			return true, nil
		})
	return files, err
}

func (ociDriver *ArtifactDriver) IsDirectory(artifact *wfv1.Artifact) (bool, error) {
	log.Info("DEBUG: IsDirectory")
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			return true, nil
		})
	return true, err
}

func isTransientOCIErr() bool {
	log.Info("DEBUG: isTransientOCIErr")
	return true
}
