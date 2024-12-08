// Load the DAG from the database, identifying orpahn nodes and tramitting them to the synchronizer.
// we must have the possibility to load an entiere round from the database to avoid to send some children before their parents that we have localy and try to fetching them from peers.
