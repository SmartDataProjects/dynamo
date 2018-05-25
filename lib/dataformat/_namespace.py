# Namespace-specific rules for e.g. object name conversions

from exceptions import ObjectError

def customize_dataset(Dataset):
    # Enumerator for dataset type.
    # Starting from 1 to play better with MySQL enums
    Dataset._data_types = ['unknown', 'production', 'test']
    for name, val in zip(Dataset._data_types, range(1, len(_data_types) + 1)):
        # e.g. Dataset.TYPE_UNKNOWN = 1
        setattr(Dataset, 'TYPE_' + name.upper(), val)

def Block_to_internal_name(name_str):
    return name_str

def Block_to_real_name(name):
    return name

def Block_to_full_name(dataset_name, block_real_name):
    return dataset_name + '/' + block_real_name

def Block_from_full_name(full_name):
    """
    @param full_name   Full name of the block
    @return  (dataset name, block internal name)
    """
    delim = full_name.find('/')
    if delim == -1:
        raise ObjectError('Invalid block name %s' % full_name)

    return full_name[:delim], full_name[delim + 1:]

def customize_block(Block):
    Block.to_internal_name = Block_to_internal_name
    Block.to_real_name = Block_to_real_name
    Block.to_full_name = Block_to_full_name
    Block.from_full_name = Block_from_full_name

def customize_blockreplica(BlockReplica):
    pass
