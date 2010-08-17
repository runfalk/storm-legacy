import transaction


class ZStormCommitter(object):

    def commit(self):
        transaction.commit()

    def rollback(self):
        transaction.abort()
