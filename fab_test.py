import time

from fabric import (
    Connection,
    SerialGroup,
    ThreadingGroup,
)

from connection_config import (
    hosts,
    key_filenames,
)

connect_kwargs = {
    "key_filename": key_filenames,
}


# paths
go_path = "$HOME/go"
libp2p_path = f"{go_path}/src/github.com/libp2p/go-libp2p"
poc_path = f"{libp2p_path}/examples/minimal"
pubsub_path = f"{go_path}/src/github.com/libp2p/go-floodsub"
go_executable_path = "/usr/local/go/bin/go"
# git related for pubsub
poc_remote = "mine"
poc_branch = "poc-testing"
pubsub_remote = "mine"
pubsub_orig_repo = "github.com/libp2p/go-floodsub"
pubsub_mine_repo = "github.com/mhchia/go-floodsub"
pubsub_mine_url = f"https://{pubsub_mine_repo}"
pubsub_branch = "gossipsub-buffersize-changed"

# set connections

connections = []
for host in hosts:
    c = Connection(
        user=host.user,
        host=host.ip,
        port=host.port,
        connect_kwargs=connect_kwargs,
    )
    # TODO: maybe can do sth to pre-connect
    connections.append(c)

def make_or_cmd(cmds):
    return " || ".join(cmds)

def make_and_cmd(cmds):
    return " && ".join(cmds)

def make_batch_cmd(cmds):
    return " ; ".join(cmds)

# cmds
cmd_set_env = make_batch_cmd([
    f"export GOPATH={go_path}",
    "export GOROOT=/usr/local/go",
    "export PATH=$PATH:$GOPATH/bin",
    "export PATH=$PATH:$GOROOT/bin",
])
cmd_pull = make_batch_cmd([
    cmd_set_env,
    "cd {0}",
    "git fetch {1}",
    "git checkout {2}",
    "git pull {1} {2}",
])

cmd_pull_poc = cmd_pull.format(poc_path, poc_remote, poc_branch)
cmd_pull_pubsub = cmd_pull.format(pubsub_path, pubsub_remote, pubsub_branch)
cmd_setup_pubsub = make_batch_cmd([
    cmd_set_env,
    # f"go get -u {pubsub_orig_repo}",
    f"cd {pubsub_path}",
    f"git remote rm {pubsub_remote}",
    f"git remote add {pubsub_remote} {pubsub_mine_url}",
])

cmd_build_poc = make_batch_cmd([
    cmd_set_env,
    cmd_pull_poc,
    "go build",
])

# g = ThreadingGroup.from_connections(connections).run("echo $GOPATH")
g = ThreadingGroup.from_connections(connections).run(
    # cmd_setup_pubsub,
    # cmd_pull_pubsub,
    cmd_build_poc
    # cmd_pull_poc
    # f"{go_executable_path} build"
)
g = ThreadingGroup.from_connections(connections).run("pwd")
print(dir(g[connections[0]]))

# def c
