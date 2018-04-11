from dynamo.dataformat import Configuration

class CopyInterface(object):
    """
    Interface to data copy application.
    """

    @staticmethod
    def get_instance(module, config = None):
        import dynamo.operation.impl as impl
        cls = getattr(impl, module)

        if not issubclass(cls, CopyInterface):
            raise RuntimeError('%s is not a subclass of CopyInterface' % module)

        return cls(config)


    def __init__(self, config = None):
        config = Configuration(config)

        self.dry_run = config.get('dry_run', False)

    def schedule_copy(self, replica, comments = ''):
        """
        Schedule and execute a copy operation.
        @param replica  DatasetReplica or BlockReplica
        @param comments Comments to be passed to the external interface.
        @return {operation_id: (approved, site, [dataset/block])}
        """

        raise NotImplementedError('schedule_copy')

    def schedule_copies(self, replica_list, comments = ''):
        """
        Schedule mass copies. Subclasses can implement efficient algorithms.
        @param replica_list  List of DatasetReplicas and BlockReplicas
        @param comments      Comments to be passed to the external interface.
        @return {operation_id: (approved, site, [dataset/block])}
        """

        request_mapping = {}
        for replica in replica_list:
            request_mapping.update(self.schedule_copy(replica, comments))

        return request_mapping

    def copy_status(self, operation_id):
        """
        Returns the completion status specified by the operation id as a
        {(site, dataset): status} dictionary.
        status can be a tuple (last_update, total, copied), or if the copy request
        is cancelled for some reason (e.g. subscription removed), None.
        """

        raise NotImplementedError('copy_status')
