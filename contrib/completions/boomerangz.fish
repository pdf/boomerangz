complete -c boomerangz -f
complete -c boomerangz -n '__fish_use_subcommand' -a 'config daemon dataset identity pairing status trigger version'
complete -c boomerangz -n '__fish_seen_subcommand_from config' -a 'check reload show'
complete -c boomerangz -n '__fish_seen_subcommand_from dataset' -a 'adopt clean inspect list reseed'
complete -c boomerangz -n '__fish_seen_subcommand_from identity' -a 'recover'
complete -c boomerangz -n '__fish_seen_subcommand_from pairing' -a 'create import list revoke'
complete -c boomerangz -l config -r -F
complete -c boomerangz -l config-dir -r -F
complete -c boomerangz -l socket -r -F
complete -c boomerangz -l apply
complete -c boomerangz -l recursive
complete -c boomerangz -l all
complete -c boomerangz -l destroy-owned-snapshots
complete -c boomerangz -s w -l watch
complete -c boomerangz -s i -l interval -r
complete -c boomerangz -l credential -r
complete -c boomerangz -l listener -r
complete -c boomerangz -l client-cert -r -F
complete -c boomerangz -l client-key -r -F
complete -c boomerangz -l scope -r -a 'status trigger replicate prune admin'
complete -c boomerangz -l expires-in -r
complete -c boomerangz -n '__fish_seen_subcommand_from list inspect version' -l json -d 'Emit JSON'
