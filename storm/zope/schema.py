import transaction

from storm.schema import Schema


class ZStormCommitter(object):

    def commit(self):
        transaction.commit()

    def rollback(self):
        transaction.abort()


class ZSchema(Schema):

    def __init__(self, creates, drops, deletes, patch_package):
        committer = ZStormCommitter()
        super(ZSchema, self).__init__(creates, drops, deletes, patch_package,
                                      committer)
