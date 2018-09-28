def compare_two(path1, path2):
    parts1 = path1.split('/')[1:] # [0] is ''
    parts2 = path2.split('/')[1:]

    ip = 0
    while ip < len(parts1) and ip < len(parts2):
        if parts1[ip] != parts2[ip]:
            break
        ip += 1

    return '/' + '/'.join(parts1[:ip])

def find_common_base(paths):
    """
    Find the "greatest common denominator" (shared base directories) of given paths. If the GCD of
    two paths is /, the two are considered to not share any base.
    @param paths    List of absolute paths

    @return list of shared base directories.
    """

    if len(paths) < 2:
        return paths

    base_directories = [paths[0]]

    for path in paths[1:]:
        for ib, base in enumerate(list(base_directories)):
            common = compare_two(base, path)

            if common != '/':
                base_directories[ib] = common
                # no other entry in base_directories should have anything in common with path
                break

        else:
            # no entry had a common base
            base_directories.append(path)

    return base_directories
