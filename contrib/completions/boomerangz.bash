_boomerangz_complete_words() {
    local words=$1 current=$2
    mapfile -t COMPREPLY < <(compgen -W "$words" -- "$current")
}

_boomerangz_complete_files() {
    local current=$1 candidate
    COMPREPLY=()
    while IFS= read -r candidate; do
        COMPREPLY+=("$candidate")
    done < <(compgen -f -- "$current")
}

_boomerangz_complete_attached_file() {
    local current=$1 prefix value candidate
    prefix=${current%%=*}=
    value=${current#*=}
    COMPREPLY=()
    while IFS= read -r candidate; do
        COMPREPLY+=("${prefix}${candidate}")
    done < <(compgen -f -- "$value")
}

_boomerangz_complete_datasets() {
    local current=$1 dataset
    COMPREPLY=()
    command -v zfs >/dev/null 2>&1 || return
    while IFS= read -r dataset; do
        [[ $dataset == "$current"* ]] && COMPREPLY+=("$dataset")
    done < <(command zfs list -H -o name -s name -t filesystem,volume 2>/dev/null)
}

_boomerangz_complete_reseed_targets() {
    local dataset=$1 current=$2 value target
    local -A seen=()
    COMPREPLY=()
    [[ -n $dataset ]] || return
    command -v zfs >/dev/null 2>&1 || return
    while IFS= read -r value; do
        [[ -n $value && $value != - ]] || continue
        while IFS= read -r target; do
            target=${target#"${target%%[![:space:]]*}"}
            target=${target%"${target##*[![:space:]]}"}
            [[ -n $target && $target == "$current"* && -z ${seen[$target]+x} ]] || continue
            seen[$target]=1
            COMPREPLY+=("$target")
        done < <(printf '%s\n' "$value" | tr ',' '\n')
    done < <(command zfs get -H -o value \
        org.boomerangz:local,org.boomerangz:remote "$dataset" 2>/dev/null)
}

_boomerangz_option_takes_value() {
    case $1 in
        --config|--config-dir|--socket|--credential|--interval|-i|--listener|\
            --client-cert|--client-key|--scope|--expires-in|--owner)
            return 0
            ;;
    esac
    return 1
}

_boomerangz_options() {
    local command=$1 subcommand=$2
    case "$command $subcommand" in
        'config check'|'config show')
            printf '%s' '-h --help --config --config-dir'
            ;;
        'config reload')
            printf '%s' '-h --help --socket'
            ;;
        'daemon ')
            printf '%s' '-h --help --config --config-dir'
            ;;
        'status ')
            printf '%s' '-h --help --config --config-dir --credential -w --watch -i --interval'
            ;;
        'trigger ')
            printf '%s' '-h --help --config --config-dir --credential'
            ;;
        'version ')
            printf '%s' '-h --help --json'
            ;;
        'pairing create')
            printf '%s' '-h --help --config --config-dir --listener --client-cert --client-key --scope --expires-in'
            ;;
        'pairing import'|'pairing list'|'pairing revoke')
            printf '%s' '-h --help --config --config-dir'
            ;;
        'dataset list'|'dataset inspect')
            printf '%s' '-h --help --config --config-dir --json'
            ;;
        'dataset adopt'|'dataset reseed')
            printf '%s' '-h --help --config --config-dir --apply'
            ;;
        'dataset clean')
            printf '%s' '-h --help --config --config-dir --recursive --all --destroy-owned-snapshots --apply'
            ;;
        'identity recover')
            printf '%s' '-h --help --config --config-dir --owner --apply'
            ;;
        *)
            printf '%s' '-h --help'
            ;;
    esac
}

_boomerangz() {
    local current previous command='' subcommand='' options word
    local positional_count=0 skip_value=0 end_options=0 saw_all=0 i
    local -a positionals=()

    current=${COMP_WORDS[COMP_CWORD]}
    previous=
    (( COMP_CWORD > 0 )) && previous=${COMP_WORDS[COMP_CWORD-1]}

    case $current in
        --config=*|--config-dir=*|--socket=*|--client-cert=*|--client-key=*)
            _boomerangz_complete_attached_file "$current"
            return
            ;;
        --scope=*)
            local scope_prefix=${current%%=*}=
            local scope_value=${current#*=}
            _boomerangz_complete_words 'status trigger replicate prune admin' "$scope_value"
            for i in "${!COMPREPLY[@]}"; do
                COMPREPLY[i]=${scope_prefix}${COMPREPLY[i]}
            done
            return
            ;;
    esac

    if _boomerangz_option_takes_value "$previous"; then
        case $previous in
            --config|--config-dir|--socket|--client-cert|--client-key)
                _boomerangz_complete_files "$current"
                ;;
            --scope)
                _boomerangz_complete_words 'status trigger replicate prune admin' "$current"
                ;;
            *)
                COMPREPLY=()
                ;;
        esac
        return
    fi

    for ((i = 1; i < COMP_CWORD; i++)); do
        word=${COMP_WORDS[i]}
        if (( skip_value )); then
            skip_value=0
            continue
        fi
        if (( ! end_options )); then
            if [[ $word == -- ]]; then
                end_options=1
                continue
            fi
            if _boomerangz_option_takes_value "$word"; then
                skip_value=1
                continue
            fi
            if [[ $word == --*=* || $word == -* ]]; then
                [[ $word == --all ]] && saw_all=1
                continue
            fi
        fi
        if [[ -z $command ]]; then
            command=$word
            continue
        fi
        case $command in
            config|dataset|identity|pairing)
                if [[ -z $subcommand ]]; then
                    subcommand=$word
                    continue
                fi
                ;;
        esac
        positionals+=("$word")
        ((positional_count++))
    done

    if [[ $current == -* ]]; then
        options=$(_boomerangz_options "$command" "$subcommand")
        _boomerangz_complete_words "$options" "$current"
        return
    fi

    if [[ -z $command ]]; then
        _boomerangz_complete_words \
            'config daemon dataset identity pairing status trigger version' "$current"
        return
    fi
    if [[ -z $subcommand ]]; then
        case $command in
            config) _boomerangz_complete_words 'check reload show' "$current" ;;
            dataset) _boomerangz_complete_words 'adopt clean inspect list reseed' "$current" ;;
            identity) _boomerangz_complete_words 'recover' "$current" ;;
            pairing) _boomerangz_complete_words 'create import list revoke' "$current" ;;
            trigger) _boomerangz_complete_datasets "$current" ;;
            *) COMPREPLY=() ;;
        esac
        return
    fi

    case "$command $subcommand" in
        'dataset inspect'|'dataset adopt')
            (( positional_count == 0 )) && _boomerangz_complete_datasets "$current" || COMPREPLY=()
            ;;
        'dataset reseed')
            if (( positional_count == 0 )); then
                _boomerangz_complete_datasets "$current"
            elif (( positional_count == 1 )); then
                _boomerangz_complete_reseed_targets "${positionals[0]}" "$current"
            else
                COMPREPLY=()
            fi
            ;;
        'dataset clean')
            if (( saw_all )); then
                COMPREPLY=()
            else
                _boomerangz_complete_datasets "$current"
            fi
            ;;
        'pairing import')
            (( positional_count == 1 )) && _boomerangz_complete_files "$current" || COMPREPLY=()
            ;;
        *)
            COMPREPLY=()
            ;;
    esac
}

complete -F _boomerangz boomerangz
