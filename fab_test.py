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
go_github_prefix = f"{go_path}/src/github.com"
go_executable_path = "/usr/local/go/bin/go"

libp2p_repo = "go-libp2p"
pubsub_repo = "go-floodsub"
poc_repo = "sharding-poc"

# repos
repos = {
    libp2p_repo: {
        "orig": {
            "owner": "libp2p",
            "remote": "origin",
            "branch": "master",
        },
    },
    pubsub_repo: {
        "orig": {
            "owner": "libp2p",
            "remote": "origin",
            "branch": "master",
        },
        "using": {
            "owner": "libp2p",
            "remote": "origin",
            "branch": "feat/gossipsub",
        }
    },
    poc_repo: {
        "orig": {
            "owner": "mhchia",
            "remote": "origin",
            "branch": "master",
        },
    },
}


def get_using_repo_info(repo):
    if "using" in repos[repo]:
        return repos[repo]["using"]
    else:
        return repos[repo]["orig"]


def make_repo_github_path(repo):
    owner = repos[repo]["orig"]["owner"]
    return "github.com/{}/{}".format(owner, repo)


def make_repo_src_path(repo):
    return "{}/src/{}".format(
        go_path,
        make_repo_github_path(repo),
    )


def make_repo_url(repo):
    return "https://{}".format(
        make_repo_github_path(repo),
    )


def make_using_repo_url(repo):
    using_repo_info = get_using_repo_info(repo)
    using_owner = using_repo_info["owner"]
    return "https://github.com/{}/{}".format(
        using_owner,
        repo,
    )


assert make_repo_github_path(poc_repo) == "github.com/mhchia/sharding-poc"
assert make_repo_src_path(poc_repo) == f"{go_path}/src/github.com/mhchia/sharding-poc"
assert make_repo_url(poc_repo) == "https://github.com/mhchia/sharding-poc"


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


def make_cmd_setup(repo):
    using_info = get_using_repo_info(repo)
    return make_batch_cmd([
        cmd_set_env,
        "go get -u {}".format(make_repo_github_path(repo)),
        "cd {}".format(make_repo_src_path(repo)),
        # f"git remote rm {pubsub_remote}",
        "git remote add {} {}".format(
            using_info["remote"],
            make_using_repo_url(repo),
        ),
    ])


def make_cmd_pull(repo):
    using_info = get_using_repo_info(repo)
    return cmd_pull_template.format(
        make_repo_src_path(repo),
        using_info["remote"],
        using_info["branch"],
    )


def make_cmd_build(repo):
    return make_batch_cmd([
        cmd_set_env,
        make_cmd_pull(repo),
        "go build",
    ])


host_conns = tuple(get_host_conns())
node_conns = tuple(get_node_conns())


def update_build_poc(conns):
    g = ThreadingGroup.from_connections(conns).run(
        make_batch_cmd([
            make_cmd_setup(libp2p_repo),
            make_cmd_pull(libp2p_repo),
            make_cmd_setup(pubsub_repo),
            make_cmd_pull(pubsub_repo),
            make_cmd_setup(poc_repo),
            make_cmd_pull(poc_repo),
            make_cmd_build(poc_repo),
        ])
    )
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
        program_cmd = f"./sharding-poc -seed {node_index} "
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
            "cd {}".format(make_repo_src_path(poc_repo)),
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
    # update_build_poc(host_conns)
    run_nodes(node_conns)
