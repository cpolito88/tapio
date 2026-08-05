"""Remoting: addressing, the wire format, and the registries behind them.

Off unless configured. A system with no `remote` settings still writes its
refs down with the system name and no host, so a peer reading one can tell
which system it names and that there is nowhere to dial.

The submodules import back into the actor package, so this file re-exports
nothing: importing it eagerly would make the two packages a cycle. Import from
the module that owns the name instead, or from `tapio` for `register_message`.
"""
