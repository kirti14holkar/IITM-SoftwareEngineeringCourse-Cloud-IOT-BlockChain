from block import Block


class DodoCoin:
    def __init__(self):
        self.mem_pool = []
        self._genesis_block = None
        self.wallets = {}
        self.difficulty_level = 4  # Added new attribute.
        # Solution 3.b.i
        # Created an attribute current_version
        self.current_version = 3
        self.__create_genesis_block()

    def __create_genesis_block(self):
        # Solution 3.b.ii
        # Passed an argument current_version
        self._genesis_block = Block(index=0, transactions=[], previous_block_hash=0,
                                    current_version=self.current_version, difficulty_level=1,
                                    metadata='The Times 03/Jan/2009 Chancellor on brink of second bailout for banks'
                                             'Genesis block using same string as bitcoin!')
        self._genesis_block.generate_hash()

    @property
    def genesis_block(self):
        return self._genesis_block

    def register_wallet(self, friendly_name, public_key):
        self.wallets[friendly_name] = public_key

    def list_wallets(self):
        for key, value in self.wallets.items():
            print(f"{key} - {value}")

    def list_pending_transactions(self):
        for transaction in self.mem_pool:
            print(transaction)

    # Added new function.
    def update_difficulty_level(self, new_level):
        self.difficulty_level = new_level

    def update_current_version(self, new_version):
        self.current_version = new_version


if __name__ == "__main__":
    from blockchain import DodoCoin
    from wallet import Wallet
    from node import Node

    dodo = DodoCoin()
    node_1 = Node("Node_1", dodo)

    sunil_wallet = Wallet('Sunil', node_1)
    harsh_wallet = Wallet('Harsh', node_1)
    dodo.register_wallet(sunil_wallet.user, sunil_wallet.public_key)
    dodo.register_wallet(harsh_wallet.user, harsh_wallet.public_key)

    sunil_wallet.initiate_transaction("Harsh", 50)
    sunil_wallet.initiate_transaction("Harsh", 20)
    dodo.list_pending_transactions()
    dodo.list_wallets()
