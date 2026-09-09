_boomerangz() {
    local current previous commands
    current=${COMP_WORDS[COMP_CWORD]}
    previous=${COMP_WORDS[COMP_CWORD-1]}
    commands="config daemon dataset identity pairing status trigger version"
    if [[ $COMP_CWORD -eq 2 ]]; then
        case "${COMP_WORDS[1]}" in
            config) commands="check reload show" ;;
            dataset) commands="adopt clean inspect list" ;;
            identity) commands="recover" ;;
            pairing) commands="create import list revoke" ;;
        esac
    fi
    case "$previous" in
        --config|--config-dir|--client-cert|--client-key|--socket)
            COMPREPLY=( $(compgen -f -- "$current") )
            return
            ;;
        --scope)
            COMPREPLY=( $(compgen -W "status trigger replicate prune admin" -- "$current") )
            return
            ;;
    esac
    if [[ $current == -* ]]; then
        COMPREPLY=( $(compgen -W "--all --apply --client-cert --client-key --config --config-dir --credential --destroy-owned-snapshots --expires-in --help --interval --json --listener --owner --recursive --scope --socket --watch" -- "$current") )
    else
        COMPREPLY=( $(compgen -W "$commands" -- "$current") )
    fi
}
complete -F _boomerangz boomerangz
