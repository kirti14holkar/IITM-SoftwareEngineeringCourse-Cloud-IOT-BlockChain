from blockchain import DodoCoin
from wallet import Wallet
from node import Node

dodo = DodoCoin()

node_1 = Node("Node-1", dodo)
node_2 = Node("Node-2", dodo)
node_3 = Node("Node-3", dodo, node_1)
node_1.connect_with_new_node(node_2, True)
node_1.connect_with_new_node(node_3, True)
node_2.connect_with_new_node(node_1, True)
node_2.connect_with_new_node(node_3, True)
node_1.show_connected_nodes()
node_2.show_connected_nodes()
node_3.show_connected_nodes()

peter_wallet = Wallet('Peter', node_1)
tony_wallet = Wallet('Tony', node_1)
strange_wallet = Wallet('Strange', node_2)
bruce_wallet = Wallet('Bruce', node_2)
steve_wallet = Wallet('Steve', node_3)
carol_wallet = Wallet('Carol', node_1)
scarlet_wallet = Wallet('Scarlet', node_3)


# Register each wallet with Blockchain
dodo.register_wallet(peter_wallet.user, peter_wallet.public_key)
dodo.register_wallet(tony_wallet.user, tony_wallet.public_key)
dodo.register_wallet(strange_wallet.user, strange_wallet.public_key)
dodo.register_wallet(bruce_wallet.user, bruce_wallet.public_key)
dodo.register_wallet(steve_wallet.user, steve_wallet.public_key)
dodo.register_wallet(carol_wallet.user, carol_wallet.public_key)
dodo.register_wallet(scarlet_wallet.user, scarlet_wallet.public_key)


transaction = peter_wallet.initiate_transaction(tony_wallet.user, 20)
# node_1.add_new_transaction(transaction)
print("\nList of pending transactions.")
dodo.list_pending_transactions()
node_1.create_new_block()
node_1.show_chain()
print()

node_2.show_chain()
print()


peter_wallet.initiate_transaction(bruce_wallet.user, 25)
# node_1.add_new_transaction(transaction)
bruce_wallet.initiate_transaction(peter_wallet.user, 50)
# node_1.add_new_transaction(transaction)
tony_wallet.initiate_transaction(bruce_wallet.user, 50)
# node_1.add_new_transaction(transaction)

node_2.create_new_block()
print()
node_1.show_chain()
print()
node_3.show_chain()
print()
node_2.show_chain()





