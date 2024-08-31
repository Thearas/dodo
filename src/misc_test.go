package src

import (
	"os"
	"path"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestFileGlob(t *testing.T) {
	t.Parallel()
	type args struct {
		paths []string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "not found",
			args: args{
				paths: []string{"not_found_file*.txt"},
			},
			want:    []string{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FileGlob(tt.args.paths)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileGlob() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FileGlob() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompressTarGz(t *testing.T) {
	chroot()
	t.Parallel()

	dstPath := "dodo-test-compress-fixture.tar.gz"
	err := CompressTarGz("fixture", dstPath, 4)
	assert.NoError(t, err)
	assert.NoError(t, os.Remove(dstPath))
}

func TestPing(t *testing.T) {
	t.Skip()
	t.Parallel()
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		host    string
		timeout time.Duration
		wantErr bool
	}{
		// {
		// 	name:    "baidu.com",
		// 	host:    "baidu.com",
		// 	timeout: 500 * time.Millisecond,
		// 	wantErr: false,
		// },
		{
			name:    "some invalid host",
			host:    "invalid.host",
			timeout: 100 * time.Millisecond,
			wantErr: true,
		},
		{
			name:    "localhost",
			host:    "localhost",
			timeout: 100 * time.Millisecond,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := Ping(tt.host, tt.timeout)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Ping() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Ping() succeeded unexpectedly")
			}
		})
	}
}

func TestGetCIDR(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		ips     []string
		want    []string
		wantErr bool
	}{
		{
			name:    "simple",
			ips:     []string{"172.20.48.119", "172.20.48.118", "172.20.48.117"},
			want:    []string{"172.20.48.0/24"},
			wantErr: false,
		},
		{
			name:    "localhost",
			ips:     []string{},
			want:    []string{GetLocalIP() + "/24"},
			wantErr: false,
		},
		{
			name:    "single",
			ips:     []string{"172.20.48.119"},
			want:    []string{"172.20.48.0/24"},
			wantErr: false,
		},
		{
			name:    "multi cidr",
			ips:     []string{"172.20.48.119", "172.20.49.118", "172.20.50.117"},
			want:    []string{"172.20.48.0/24", "172.20.49.0/24", "172.20.50.0/24"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := GetCIDR(tt.ips...)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("GetCIDR() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("GetCIDR() succeeded unexpectedly")
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

var chrootLock = &atomic.Bool{}

func chroot() {
	if !chrootLock.CompareAndSwap(false, true) {
		return
	}

	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "..")
	if err := os.Chdir(dir); err != nil {
		panic(err)
	}
}

func disableLog() {
	logrus.SetLevel(logrus.ErrorLevel)
}
