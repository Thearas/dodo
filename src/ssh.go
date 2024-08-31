package src

import (
	"context"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	scp "github.com/bramvdbogaerde/go-scp"
	"github.com/bramvdbogaerde/go-scp/auth"
	"github.com/sirupsen/logrus"
	"github.com/vbauerster/mpb/v8"
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
	clientConfig.Timeout = 10 * time.Second

	return &clientConfig, host, user, urlParsed.Path, nil
}

func SshLs(privKey, remoteUrl string) ([]string, error) {
	_, _, _, remotePath, _ := sshClientConfig(remoteUrl, privKey)
	stdout, _, err := SshExec(privKey, remoteUrl, `ls -l `+remotePath+` | awk '{print $NF}'`)
	if err != nil {
		return nil, err
	}

	return strings.Split(string(stdout), "\n"), nil
}

func SshExec(privKey, remoteUrl, command string) (string, string, error) {
	clientConfig, host, _, _, err := sshClientConfig(remoteUrl, privKey)
	if err != nil {
		return "", "", err
	}

	client, err := ssh.Dial("tcp", host, clientConfig)
	if err != nil {
		logrus.Debugln("couldn't establish ssh connection to the remote server", err)
		return "", "", err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		logrus.Debugln("couldn't establish ssh session to the remote server", err)
		return "", "", err
	}
	defer session.Close()

	var (
		stdout strings.Builder
		stderr strings.Builder
	)
	session.Stdout = &stdout
	session.Stderr = &stderr

	if err := session.Run(command); err != nil {
		return "", stderr.String(), err
	}

	return stdout.String(), stderr.String(), nil
}

// ScpFromRemote copies a file from a remote server to the local machine using scp.
//
//	privKey is the path to the private key to use for authentication.
//	remoteUrl is the address of file on the remote server, format ssh://user:password@host:port/path.
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

	err = client.CopyFromRemotePassThru(ctx, f, remotePath, func(r io.Reader, total int64) io.Reader {
		return BytesProgressBar(total, strings.Split(host, ":")[0], "downloading").ProxyReader(r)
	})
	if err != nil {
		logrus.Errorf("Error while copying file from host %s, err: %v", host, err)
		return err
	}

	return nil
}

func ScpToRemote(ctx context.Context, privKey, localPath, remoteUrl string) error {
	clientConfig, host, user, remotePath, err := sshClientConfig(remoteUrl, privKey)
	if err != nil {
		return err
	}

	if logrus.GetLevel() < logrus.DebugLevel {
		logrus.Infof("uploading %s to %s", localPath, remotePath)
	} else {
		logrus.Infof("uploading %s to %s@%s%s", localPath, user, host, remotePath)
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

	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	err = client.CopyFromFilePassThru(ctx, *f, remotePath, "0655", func(r io.Reader, total int64) io.Reader {
		// return BytesProgressBar(total, strings.Split(host, ":")[0], "uploading").ProxyReader(r)
		return &proxyReader{
			Reader: r,
			bar:    BytesProgressBar(total, strings.Split(host, ":")[0], "uploading"),
		}
	})
	if err != nil {
		logrus.Errorf("Error while copying file to host %s, err: %v", host, err)
		return err
	}

	return nil
}

type proxyReader struct {
	io.Reader
	bar *mpb.Bar
}

func (x proxyReader) Read(p []byte) (int, error) {
	n, err := x.Reader.Read(p)
	x.bar.IncrBy(n)
	return n, err
}
