package tolerance

import (
	"fmt"
	"github.com/odpf/predator/protocol"
	"golang.org/x/crypto/ssh"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	gitssh "gopkg.in/src-d/go-git.v4/plumbing/transport/ssh"
	"io/ioutil"
	"os"
	"path/filepath"
)

const (
	// base directory that holds the all git repositories that
	// are checked out
	gitCheckoutBaseDir = ""

	// naming template for folders that hold git repo contents
	gitCheckoutDirPrefix = "predator-git-repository-"
)

// GitAuthPrivateKey creates authentication configuration using
// a private, pem encoded key. This can be used for initialising
// a GitRepository via NewGitRepository
func GitAuthPrivateKey(pem []byte) (protocol.GitAuth, error) {
	key, err := gitssh.NewPublicKeys("git", pem, "")
	if err != nil {
		return nil, fmt.Errorf("GitAuthPrivateKey: %w", err)
	}

	key.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	return key, nil
}

// GitRepository provides a FS like access to remote git repositories
// It clones the repository locally, and provides a FS interface over the
// contents.
type GitRepository struct {
	url        string           // url of the git repo
	auth       protocol.GitAuth // auth
	pathPrefix string           //prefix directory is needed to get based directory of file repository
}

// Checkout returns a Git FileSystem at a certain commit
// Remember to call Close on the returned FileSystem, otherwise
// the checked out repository on disk will not be not be deleted
// if commit is empty, checksout master
func (repo *GitRepository) Checkout(commit string) (protocol.FileStore, error) {

	// create a temporary directory and clone the repo inside it
	tempDir, err := ioutil.TempDir(gitCheckoutBaseDir, gitCheckoutDirPrefix)
	if err != nil {
		return nil, err
	}
	gitRepo, err := git.PlainClone(tempDir, false, &git.CloneOptions{
		URL:  repo.url,
		Auth: repo.auth,
	})
	if err != nil {
		return nil, err
	}

	// make sure to cleanup if something fails
	defer func() {
		if err != nil {
			os.RemoveAll(tempDir)
			err = fmt.Errorf("Checkout: %v", err)
		}
	}()

	//don't checkout and stay to master if provided empty
	if commit != "" {
		// verify that the commit is valid
		commitHash := plumbing.NewHash(commit)
		_, err = gitRepo.CommitObject(commitHash)
		if err != nil {
			return nil, err
		}

		// checkout the required commit
		wt, err := gitRepo.Worktree()
		if err != nil {
			return nil, err
		}
		checkoutOpts := &git.CheckoutOptions{
			Hash: commitHash,
		}
		if err = wt.Checkout(checkoutOpts); err != nil {
			return nil, err
		}
	}
	rootPath := filepath.Join(tempDir, repo.pathPrefix)
	return &LocalFileStorage{baseDir: rootPath}, nil
}

//GitRepositoryFactory creator of GitRepository
type GitRepositoryFactory struct {
	auth protocol.GitAuth
}

func NewGitRepositoryFactory(auth protocol.GitAuth) *GitRepositoryFactory {
	return &GitRepositoryFactory{auth: auth}
}

func (g *GitRepositoryFactory) CreateWithPrefix(url string, pathPrefix string) protocol.GitRepository {
	return &GitRepository{
		url:        url,
		auth:       g.auth,
		pathPrefix: pathPrefix,
	}
}

//Create create GitRepository
func (g *GitRepositoryFactory) Create(url string) protocol.GitRepository {
	return &GitRepository{
		url:  url,
		auth: g.auth,
	}
}
