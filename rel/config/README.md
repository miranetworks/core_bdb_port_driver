The default `sys.config` and `vm.args` files are used on a `make run` and on production.
To provide host specific production config, create a subdirectory with the short hostname and config file(s) as follows:

    rel/config/
        ├── sname (short hostname of the production box)
        │   ├── sys.config
        │   └ ─ vm.args (optional)
        ├── sys.config
        └── vm.args

