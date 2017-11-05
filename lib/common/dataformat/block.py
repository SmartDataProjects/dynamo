class Block(object):
    __slots__ = ['name', 'dataset', 'size', 'num_files', 'is_open']

    @staticmethod
    def translate_name(name_str):
        # block name format: [8]-[4]-[4]-[4]-[12] where [n] is an n-digit hex.
        return int(name_str.replace('-', ''), 16)

    def __init__(self, name, dataset = None, size = 0, num_files = 0, is_open = False):
        self.name = name
        self.dataset = dataset
        self.size = size
        self.num_files = num_files
        self.is_open = is_open

    def __str__(self):
        return 'Block %s#%s (size=%d, num_files=%d, is_open=%s)' % (self.dataset.name, self.real_name(), self.size, self.num_files, self.is_open)

    def real_name(self):
        full_string = hex(self.name).replace('0x', '')[:-1] # last character is 'L'
        if len(full_string) < 32:
            full_string = '0' * (32 - len(full_string)) + full_string

        return full_string[:8] + '-' + full_string[8:12] + '-' + full_string[12:16] + '-' + full_string[16:20] + '-' + full_string[20:]

    def clone(self, **kwd):
        return Block(
            self.name,
            self.dataset if 'dataset' not in kwd else kwd['dataset'],
            self.size if 'size' not in kwd else kwd['size'],
            self.num_files if 'num_files' not in kwd else kwd['num_files'],
            self.is_open if 'is_open' not in kwd else kwd['is_open']
        )
