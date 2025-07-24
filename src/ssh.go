package src

import (
	"context"
	"net/url"
	"os"
	"path"
	"strings"

	scp "github.com/bramvdbogaerde/go-scp"
	"github.com/bramvdbogaerde/go-scp/auth"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
)

func sshClientConfig(remoteUrl, privKey string) (cfg *ssh.ClientConfig, host, user, path string, err error) {
	urlParsed, err := url.Parse(remoteUrl)
	if err != nil {
		return
	}
	user = urlParsed.User.Username()
	host = urlParsed.Host
	password, passInAddr := urlParsed.User.Password()

	var clientConfig ssh.ClientConfig
	if passInAddr {
		clientConfig, _ = auth.PasswordKey(user, password, ssh.InsecureIgnoreHostKey())
	} else {
		clientConfig, err = auth.PrivateKey(user, privKey, ssh.InsecureIgnoreHostKey())
		if err != nil {
			return
		}
	}

	return &clientConfig, host, user, urlParsed.Path, nil
}

func SshLs(_ context.Context, privKey, remoteUrl string) ([]string, error) {
	clientConfig, host, _, remotePath, err := sshClientConfig(remoteUrl, privKey)
	if err != nil {
		return nil, err
	}

	client, err := ssh.Dial("tcp", host, clientConfig)
	if err != nil {
		logrus.Debugln("couldn't establish ssh connection to the remote server", err)
		return nil, err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		logrus.Debugln("couldn't establish ssh session to the remote server", err)
		return nil, err
	}

	out, err := session.Output(`ls -l ` + remotePath + ` | awk '{print $NF}'`)
	if err != nil {
		panic(err)
	}

	return strings.Split(string(out), "\n"), nil
}

// ScpFromRemote copies a file from a remote server to the local machine using scp.
//
//	privKey is the path to the private key to use for authentication.
//	remoteUrl is the address of file on the remote server, format user@host:port/path.
//	localPath is the path of the local file to copy to.
func ScpFromRemote(ctx context.Context, privKey, remoteUrl, localPath string) error {
	clientConfig, host, user, remotePath, err := sshClientConfig(remoteUrl, privKey)
	if err != nil {
		return err
	}

	if logrus.GetLevel() < logrus.DebugLevel {
		logrus.Infof("downloading %s to %s", remotePath, localPath)
	} else {
		logrus.Infof("downloading %s@%s%s to %s", user, host, remotePath, localPath)
	}

	// Create a new SCP client
	client := scp.NewClient(host, clientConfig)
	if err := client.Connect(); err != nil {
		logrus.Debugln("couldn't establish ssh to the remote server, try using private key authentication", err)

		// try private key authentication
		cfg, err_ := auth.PrivateKey(user, privKey, ssh.InsecureIgnoreHostKey())
		if err_ != nil {
			return err
		}
		client = scp.NewClient(host, &cfg)
		if err_ = client.Connect(); err_ != nil {
			logrus.Debugln("couldn't establish ssh to the remote server again with private key", err)
			return err
		}
	}
	defer client.Close()

	if err := os.MkdirAll(path.Dir(localPath), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(localPath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	err = client.CopyFromRemote(ctx, f, remotePath)
	if err != nil {
		logrus.Errorf("Error while copying file from host %s, err: %v", host, err)
		return err
	}

	return nil
}
