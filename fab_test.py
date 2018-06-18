import time

from cytoolz import dicttoolz

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
poc_branch = "temp-poc-testing"
# poc_branch = "1dc628d96810945f71dcda9d304965907fb961ae"
pubsub_remote = "mine"
pubsub_orig_repo = "github.com/libp2p/go-floodsub"
pubsub_mine_repo = "github.com/mhchia/go-floodsub"
pubsub_mine_url = f"https://{pubsub_mine_repo}"
pubsub_branch = "gossipsub-buffersize-changed"

# TODO: hacky way to make use of `ThreadingGroup`
class CustomConnection(Connection):

    node_index = None

    def __init__(self, *args, **kwargs):
        if 'node_index' not in kwargs:
            raise ValueError("init error")
        # self.node_index = kwargs['node_index']
        self.node_index = kwargs['node_index']
        orig_kwargs = dicttoolz.dissoc(kwargs, 'node_index')
        super().__init__(
            *args,
            **orig_kwargs,
        )

    def run(self, *args, **kwargs):
        custom_kwargs = kwargs.get('custom_kwargs', None)
        if (custom_kwargs is not None) and (self.node_index in custom_kwargs):
            custom_cmd = custom_kwargs[self.node_index]
            orig_kwargs = dicttoolz.dissoc(kwargs, 'custom_kwargs')
            return super().run(custom_cmd, **orig_kwargs)
        return super().run(*args, **kwargs)


# specify which host a node locates
# e.g. hosts = [h0, h1, h2]
#      node_host_index_map = [h0, h1, h0, h2]
# (number of nodes) == len(node_host_index_map)
# node_host_index_map = tuple(range(len(hosts)))
# node_host_index_map = (0, 1, 2, 1, 0, 2, 0, 1, 2)
node_host_index_map = (0,1,0,1,0,1)
node_target = {
    i+1: i for i in range(len(node_host_index_map) - 1)
}
node_send_collation = {
    (len(node_host_index_map) - 1): (100, 1, 1000),
}


def get_host_conns():
    for node_index, host in enumerate(hosts):
        yield CustomConnection(
            user=host.user,
            host=host.ip,
            port=host.port,
            node_index=node_index,
            connect_kwargs=connect_kwargs,
        )


def get_node_conns():
    for node_index, host_index in enumerate(node_host_index_map):
        if host_index >= len(hosts):
            raise ValueError(
                "host_index {} >= len(hosts)={}".format(
                    host_index,
                    len(hosts)
                )
            )
        yield CustomConnection(
            user=hosts[host_index].user,
            host=hosts[host_index].ip,
            port=hosts[host_index].port,
            node_index=node_index,
            connect_kwargs=connect_kwargs,
        )


def make_or_cmd(cmds):
    return " ( " + " || ".join(cmds) + " ) "


def make_and_cmd(cmds):
    return " ( " + " && ".join(cmds) + " ) "


def make_batch_cmd(cmds):
    return " ; ".join(cmds)


# constants
SCALE = 2

# cmds
cmd_set_env = make_batch_cmd([
    f"export GOPATH={go_path}",
    "export GOROOT=/usr/local/go",
    "export PATH=$PATH:$GOPATH/bin",
    "export PATH=$PATH:$GOROOT/bin",
])
cmd_pull_template = make_batch_cmd([
    cmd_set_env,
    "cd {0}",
    "git fetch {1}",
    "git checkout {2}",
    "git pull {1} {2}",
])

cmd_pull_poc = cmd_pull_template.format(poc_path, poc_remote, poc_branch)
cmd_pull_pubsub = cmd_pull_template.format(pubsub_path, pubsub_remote, pubsub_branch)
cmd_setup_pubsub = make_batch_cmd([
    cmd_set_env,
    f"go get -u {pubsub_orig_repo}",
    f"cd {pubsub_path}",
    # f"git remote rm {pubsub_remote}",
    f"git remote add {pubsub_remote} {pubsub_mine_url}",
])

cmd_build_poc = make_batch_cmd([
    cmd_set_env,
    cmd_pull_poc,
    "go build",
])


host_conns = tuple(get_host_conns())
node_conns = tuple(get_node_conns())


def update_build_poc(conns):
    g = ThreadingGroup.from_connections(conns).run(cmd_build_poc)
    for conn in conns:
        result = g[conn]
        if not result.ok:
            raise ValueError(
                "building failed in conn {}, stdout=\"{}\", stderr=\"{}\"".format(
                    conn,
                    result.stdout,
                    result.stderr,
                )
            )


def run_nodes(conns):
    commands = {}
    for node_index, _ in enumerate(conns):
        program_cmd = f"./minimal -seed {node_index} "
        if node_index in node_target:
            target_node_index = node_target[node_index]
            target_node_host_index = node_host_index_map[target_node_index]
            program_cmd += "-target-seed {} -target-ip {} ".format(
                target_node_index,
                hosts[target_node_host_index].ip,
            )
        if node_index in node_send_collation:
            program_cmd += "-send {} ".format(
                ",".join(
                    map(
                        str,
                        node_send_collation[node_index],
                    )
                )
            )
        node_cmd = make_and_cmd([
            f"cd {poc_path}",
            "sleep {}".format(node_index * SCALE),
            program_cmd,
        ])
        commands[node_index] = node_cmd
    print(conns)
    print(hosts)
    print(commands)
    g = ThreadingGroup.from_connections(node_conns).run(custom_kwargs=commands)
    print(g)


if __name__ == "__main__":
    update_build_poc(host_conns)
    run_nodes(node_conns)
