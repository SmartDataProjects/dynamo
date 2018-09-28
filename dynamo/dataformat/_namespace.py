# Namespace-specific rules for e.g. object name conversions

from exceptions import ObjectError

def Dataset_format_software_version(value):
    return value

def Block_to_internal_name(name_str):
    return name_str

def Block_to_real_name(name):
    return name

def Block_to_full_name(dataset_name, block_real_name):
    return dataset_name + '#' + block_real_name

def Block_from_full_name(full_name):
    """
    @param full_name   Full name of the block
    @return  (dataset name, block internal name)
    """
    delim = full_name.find('#')
    if delim == -1:
        raise ObjectError('Invalid block name %s' % full_name)

    return full_name[:delim], Block_to_internal_name(full_name[delim + 1:])

def customize_dataset(Dataset):
    # Enumerator for dataset type.
    # Starting from 1 to play better with MySQL enums
    Dataset._data_types = ['unknown', 'production', 'test']
    for name, val in zip(Dataset._data_types, range(1, len(Dataset._data_types) + 1)):
        # e.g. Dataset.TYPE_UNKNOWN = 1
        setattr(Dataset, 'TYPE_' + name.upper(), val)

    Dataset.SoftwareVersion.field_names = ('version',)

    Dataset.format_software_version = staticmethod(Dataset_format_software_version)

def customize_block(Block):
    Block.to_internal_name = staticmethod(Block_to_internal_name)
    Block.to_real_name = staticmethod(Block_to_real_name)
    Block.to_full_name = staticmethod(Block_to_full_name)
    Block.from_full_name = staticmethod(Block_from_full_name)

def customize_file(File):
    pass

def customize_blockreplica(BlockReplica):
    pass
