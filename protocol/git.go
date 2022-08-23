package protocol

import (
	"gopkg.in/src-d/go-git.v4/plumbing/transport"
	"regexp"
)

//GitSshUrlPattern is regex pattern of supported git ssh url format
//The supported is git ssh format git@www.git.com:group/repo.git
//this can be used to verify the supported format
var GitSshUrlPattern = regexp.MustCompile(`^git@.+\.git$`)

// GitAuth describes authentication configuration for GitRepository
type GitAuth interface {
	transport.AuthMethod
}

//GitInfo git repository information
type GitInfo struct {

	//URL is a git url it must comply GitSshUrlPattern pattern format
	//this will be used to do git clone using ssh protocol
	URL string

	//CommitID is commit id that will be to be checked out
	//empty string means checking out latest revision
	CommitID string

	//PathPrefix is path prefix on git repository where the predator spec root directory structure is located
	//for example using Default directory structure, the files is flatly placed on a folder
	//if git url is git@github.com:username/project.git then in the repository the spec files placed under PathPrefix/ dir
	PathPrefix string
}

//GitRepository a git repository
type GitRepository interface {
	Checkout(commit string) (FileStore, error)
}

//GitRepositoryFactory creator of GitRepository
type GitRepositoryFactory interface {
	Create(url string) GitRepository
	CreateWithPrefix(url string, pathPrefix string) GitRepository
}
