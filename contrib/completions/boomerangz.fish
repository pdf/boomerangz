complete -c boomerangz -f
complete -c boomerangz -n '__fish_use_subcommand' -a 'config daemon dataset identity pairing status trigger version'

function __boomerangz_needs_subcommand
    set -l group $argv[1]
    set -e argv[1]
    __fish_seen_subcommand_from $group; and not __fish_seen_subcommand_from $argv
end

function __boomerangz_list_datasets
    command -q zfs; or return
    command zfs list -H -o name -s name -t filesystem,volume 2>/dev/null
end

complete -c boomerangz -n '__boomerangz_needs_subcommand config check reload show' -a 'check reload show'
complete -c boomerangz -n '__boomerangz_needs_subcommand dataset adopt clean inspect list reseed' -a 'adopt clean inspect list reseed'
complete -c boomerangz -n '__boomerangz_needs_subcommand identity recover' -a 'recover'
complete -c boomerangz -n '__boomerangz_needs_subcommand pairing create import list revoke' -a 'create import list revoke'
complete -c boomerangz -n '__fish_seen_subcommand_from trigger inspect adopt clean reseed' -a '(__boomerangz_list_datasets)' -d 'ZFS dataset'
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
complete -c boomerangz -l owner -r
complete -c boomerangz -n '__fish_seen_subcommand_from status version; or begin; __fish_seen_subcommand_from dataset; and __fish_seen_subcommand_from list inspect; end' -l json -d 'Emit JSON'
