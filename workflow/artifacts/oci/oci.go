package oci

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"

	"time"

	"github.com/oracle/oci-go-sdk/v65/objectstorage"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	ociauth "github.com/oracle/oci-go-sdk/v65/common/auth"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/wait"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	waitutil "github.com/argoproj/argo-workflows/v3/util/wait"
	"github.com/argoproj/argo-workflows/v3/workflow/artifacts/common"
)

//const tenancyOCID string = ""
//const userOCID string = ""
//const region string = "us-phoenix-1"
//const fingerprint string = ""

// const compartmentOCID string = ""
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
	log.Infof("DEBUG initializeConfigurationProvider")
	var configurationProvider ocicommon.ConfigurationProvider

	// We accept 3 types of auth. 
	// 1. instance_principal, 2. k8s_secret, 3. raw
	// default to instance principal
	switch ociDriver.Provider {
	case "instance_principal":
		fmt.Println("DEBUG: Instance Principal: ", ociDriver.Provider)
		log.Infof("DEBUG: Instance Principal: %v", ociDriver.Provider)
		configurationProvider = initializeConfigurationProvider()
	case "k8s_secret":
		fmt.Println("DEBUG: Secrets: ", ociDriver.SecretKey)
		log.Infof("DEBUG: Secrets: %v", ociDriver.SecretKey)
		fmt.Println("DEBUG: Access: ", ociDriver.AccessKey)
		log.Infof("DEBUG: Access: %v", ociDriver.AccessKey)
		access := strings.TrimSpace(ociDriver.AccessKey)
		fmt.Println("DEBUG: AccessTrim: ", access)
		log.Infof("DEBUG: AccessTrim: %v", access)
		configurationProvider = ocicommon.NewRawConfigurationProvider(ociDriver.TenancyOCID, ociDriver.UserOCID, ociDriver.Region, access, ociDriver.SecretKey, nil)
	case "raw":
		fmt.Println("DEBUG: raw: ", ociDriver.SecretKey)
		log.Infof("DEBUG: raw: %v", ociDriver.SecretKey)
		configurationProvider = ocicommon.NewRawConfigurationProvider(ociDriver.TenancyOCID, ociDriver.UserOCID, ociDriver.Region, ociDriver.Fingerprint, ociDriver.PrivateKey, nil)
	default:
		fmt.Println("DEBUG: Provider not set, default to instance principal")
		log.Infof("DEBUG: Provider not set, default to instance principal")
		configurationProvider = initializeConfigurationProvider()
	}

	storageClient, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(configurationProvider)
	if err != nil {
		log.Errorf("Object client error %s", err)
	}

	ctx := context.Background()
	return ctx, storageClient
}

// instance principal authentication
func initializeConfigurationProvider() ocicommon.ConfigurationProvider {
	fmt.Println("DEBUG: INSTANCE PRINCIPAL configuration provider")
	log.Infof("DEBUG: INSTANCE Principal configurationProvider")
	configurationProvider, err := ociauth.InstancePrincipalConfigurationProvider()
	if err != nil {
		fmt.Println("error in instance principal config provider ", err)
	}
	return configurationProvider
}

func (ociDriver *ArtifactDriver) Load(inputArtifact *wfv1.Artifact, path string) error {
	log.Infof("DEBUG Load")

	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			log.Infof("OCI Load path: %s, key %s", path, inputArtifact.OCI.Key)
			return true, nil
		})

	return err
}

func (ociDriver *ArtifactDriver) OpenStream(artifact *wfv1.Artifact) (io.ReadCloser, error) {
	log.Infof("DEBUG OpenStream")
	return common.LoadToStream(artifact, ociDriver)
}

func (ociDriver *ArtifactDriver) Save(path string, outputArtifact *wfv1.Artifact) error {
	log.Infof("DEBUG Save")
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			log.Infof("OCI Save path: #{path}, key: #{outputArtifact.OCI.Key")
			ctx, storageClient := ociDriver.initializeConfigurationProvider()

			namespace := getNamespace(ctx, storageClient)
			bucketName := outputArtifact.OCI.OCIBucket.Bucket
			objectName := outputArtifact.OCI.Key

			fmt.Println("SAVING" + namespace + "/" + bucketName + "/" + objectName)
			log.Infof("SAVING" + namespace + "/" + bucketName + "/" + objectName)

			err := putObject(ctx, storageClient, namespace, bucketName, objectName, nil)
			if err != nil {
				log.Errorf("Error putting object: %v", err)
			}

			return true, nil
		})
	return err
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

// upload a local object or dir to OCI storage
func putObject(ctx context.Context, c objectstorage.ObjectStorageClient, namespace, bucketname, objectname string, metadata map[string]string) error {
	//var contentlen int64
	contentlen := 1024 * 1000

	filepath, filesize := WriteTempFileOfSize(int64(contentlen))
	filename := path.Base(filepath)

	defer func() {
		os.Remove(filename)
	}()

	file, e := os.Open(filepath)
	if e != nil {
		log.Infof("Error opening file: %s", e.Error())
	}
	defer file.Close()
	fatalIfError(e)

	request := objectstorage.PutObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketname,
		ObjectName:    &objectname,
		ContentLength: &filesize,
		PutObjectBody: file,
		OpcMeta:       metadata,
	}
	_, err := c.PutObject(ctx, request)
	fmt.Println("Put object ", objectname, " in bucket ", bucketname)
	return err
}

func WriteTempFileOfSize(filesize int64) (fileName string, fileSize int64) {
	hash := sha256.New()
	f, _ := ioutil.TempFile("", "OCIGOSDKSampleFile")
	ra := rand.New(rand.NewSource(time.Now().UnixNano()))
	defer f.Close()
	writer := io.MultiWriter(f, hash)
	written, _ := io.CopyN(writer, ra, filesize)
	fileName = f.Name()
	fileSize = written
	return
}

func (ociDriver *ArtifactDriver) Delete(artifact *wfv1.Artifact) error {
	log.Infof("DEBUG Delete")
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			log.Infof("OCI Delete: #{outputArtifact.OCI.Key")
			ctx, storageClient := ociDriver.initializeConfigurationProvider()

			namespace := getNamespace(ctx, storageClient)
			bucketName := artifact.OCI.OCIBucket.Bucket
			objectName := artifact.OCI.Key

			fmt.Println("DELETING: " + namespace + "/" + bucketName + "/" + objectName)
			log.Infof("DELETING" + namespace + "/" + bucketName + "/" + objectName)

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
	var files []string
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			return true, nil
		})
	return files, err
}

func (ociDriver *ArtifactDriver) IsDirectory(artifact *wfv1.Artifact) (bool, error) {
	err := waitutil.Backoff(defaultRetry,
		func() (bool, error) {
			return true, nil
		})
	return true, err
}

func isTransientOCIErr() bool {
	return true
}
