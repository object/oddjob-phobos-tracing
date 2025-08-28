BEGIN TRANSACTION

DECLARE @OrderingOffset int
SELECT @OrderingOffset = MAX(Ordering) FROM EventJournal

SET IDENTITY_INSERT EventJournal ON

INSERT INTO EventJournal (Ordering,PersistenceId,SequenceNr,Timestamp,IsDeleted,Manifest,Payload,Tags,SerializerId)
SELECT Ordering+@OrderingOffset,PersistenceId,SequenceNr,Timestamp,IsDeleted,Manifest,Payload,Tags,SerializerId
FROM LiveToVodEventJournal
WHERE PersistenceId LIKE 'ltvc:%'

INSERT INTO EventJournal (Ordering,PersistenceId,SequenceNr,Timestamp,IsDeleted,Manifest,Payload,Tags,SerializerId)
SELECT Ordering+@OrderingOffset,'ltv:reminder',SequenceNr,Timestamp,IsDeleted,Manifest,Payload,Tags,SerializerId
FROM LiveToVodEventJournal
WHERE PersistenceId = 'reminder'

SET IDENTITY_INSERT EventJournal OFF

INSERT INTO SnapshotStore (PersistenceId,SequenceNr,Timestamp,Manifest,Snapshot,SerializerId)
SELECT 'ltv:reminder',SequenceNr,Timestamp,Manifest,Snapshot,SerializerId
FROM LiveToVodSnapshotStore
WHERE PersistenceId = 'reminder'

COMMIT TRANSACTION 
