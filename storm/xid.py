class Xid(object):

    def __init__(self, format_id, global_transaction_id, branch_qualifier):
        self.format_id = format_id
        self.global_transaction_id = global_transaction_id
        self.branch_qualifier = branch_qualifier
