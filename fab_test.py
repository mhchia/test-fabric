import argparse
import time

from sys import platform

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

if platform == "Darwin":
    go_root = "/usr/local/Cellar/go/1.10.2/libexec"
else:
    go_root = "/usr/local/go"

go_github_prefix = f"{go_path}/src/github.com"

libp2p_repo = "go-libp2p"
pubsub_repo = "go-floodsub"
poc_repo = "sharding-p2p-poc"

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
            "owner": "mhchia",
            "remote": "mine",
            "branch": "gossipsub-big-buffer",
        }
    },
    poc_repo: {
        "orig": {
            "owner": "mhchia",
            "remote": "origin",
            "branch": "test-speed",
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


assert make_repo_github_path(poc_repo) == "github.com/mhchia/sharding-p2p-poc"
assert make_repo_src_path(poc_repo) == f"{go_path}/src/github.com/mhchia/sharding-p2p-poc"
assert make_repo_url(poc_repo) == "https://github.com/mhchia/sharding-p2p-poc"


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


# FIXME: here is the config
# specify which host a node locates
# node_host_index_map = (0,1,1,0,0,1,1,0,0,1,)
node_host_index_map = (0,0,)
# specify which node a node add peers to
node_target = {
    i+1: i for i in range(len(node_host_index_map) - 1)
}
# specify which nodes are sending collations
node_subscribing_shards = {i:[1] for i in range(len(node_host_index_map))}
# shard_id, num_collations, collation_size, period
node_send_collation = {
    (len(node_host_index_map) - 1): (1, 10, 1000000, 0),
}

# requirements
# gcc, ...


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
SCALE = 3

# cmds
cmd_set_env = make_batch_cmd([
    f"export GOPATH={go_path}",
    f"export GOROOT={go_root}",
    "export PATH=$PATH:$GOPATH/bin",
    "export PATH=$PATH:$GOROOT/bin",
])
cmd_pull_template = make_batch_cmd([
    cmd_set_env,
    "cd {0}",
    "git fetch {1}",
    "git checkout master",
    "git branch -D {2}",
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
        "cd {0}".format(make_repo_src_path(repo)),
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


exe_name = "sharding-p2p-poc"


def run_servers(conns):
    commands = {}
    for node_index, _ in enumerate(conns):

        linux_command = f"script -f -c './{exe_name} -seed={node_index}' poc_{node_index}.out"
        osx_command = f"script -q /dev/null ./{exe_name} -seed={node_index} 2>&1 1>poc_{node_index}.out /dev/null"


        program_cmd = make_batch_cmd([
            f"killall -9 {exe_name}",
            "sleep 1",
            f"screen -d -m bash -c \"if [  \"$(uname)\" == \"Darwin\"  ]; then {osx_command}; else {linux_command}; fi\"",
        ])

        node_cmd = make_and_cmd([
            "cd {}".format(make_repo_src_path(poc_repo)),
            program_cmd,
        ])
        commands[node_index] = node_cmd

    g = ThreadingGroup.from_connections(node_conns).run(custom_kwargs=commands,echo=True)
    print(g)


def addpeer(conns):
    commands = {}
    exact_conns = []
    for node_index, conn in enumerate(conns):
        program_cmd = f"./{exe_name} -seed={node_index} -client "
        if node_index not in node_target:
            continue
        target_node_index = node_target[node_index]
        target_node_host_index = node_host_index_map[target_node_index]
        program_cmd += "addpeer {} {} ".format(
            hosts[target_node_host_index].ip,
            target_node_index,
        )
        node_cmd = make_and_cmd([
            "cd {}".format(make_repo_src_path(poc_repo)),
            program_cmd,
        ])
        exact_conns.append(conn)
        commands[node_index] = node_cmd
    print(commands)
    g = ThreadingGroup.from_connections(exact_conns).run(custom_kwargs=commands)
    print(g)


def subshard(conns):
    commands = {}
    exact_conns = []
    for node_index, conn in enumerate(conns):
        program_cmd = make_batch_cmd([
            f"./{exe_name} -seed={node_index} -client "
        ])
        if node_index not in node_subscribing_shards:
            continue
        program_cmd += "subshard {}".format(
            " ".join(
                map(str, node_subscribing_shards[node_index])
            )
        )
        node_cmd = make_and_cmd([
            "cd {}".format(make_repo_src_path(poc_repo)),
            program_cmd,
        ])
        exact_conns.append(conn)
        commands[node_index] = node_cmd
    print(commands)
    g = ThreadingGroup.from_connections(exact_conns).run(custom_kwargs=commands)
    print(g)


def broadcastcollation(conns):
    commands = {}
    exact_conns = []
    for node_index, conn in enumerate(conns):
        program_cmd = make_batch_cmd([
            f"./{exe_name} -seed={node_index} -client "
        ])
        if node_index not in node_send_collation:
            continue
        program_cmd += "broadcastcollation {}".format(
            " ".join(
                map(str, node_send_collation[node_index])
            )
        )
        node_cmd = make_and_cmd([
            "cd {}".format(make_repo_src_path(poc_repo)),
            program_cmd,
        ])
        exact_conns.append(conn)
        commands[node_index] = node_cmd
    print(commands)
    g = ThreadingGroup.from_connections(exact_conns).run(custom_kwargs=commands)
    print(g)


rpcs = {
    "addpeer": addpeer,
    "broadcastcollation": broadcastcollation,
    "subshard": subshard,
    # "unsubshard": unsubshard,
    # "getsubshard": getsubshard,
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", help="client or server")
    args = parser.parse_args()
    mode = args.mode
    if mode == "server":
        run_servers(node_conns)
    elif mode == "sync":
        update_build_poc(host_conns)
    elif mode in rpcs:
        rpcs[mode](node_conns)
    else:
        raise ValueError("wrong mode")
