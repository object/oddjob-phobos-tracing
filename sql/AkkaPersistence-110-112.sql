ALTER TABLE EventJournal DROP CONSTRAINT PK_EventJournal;
ALTER TABLE EventJournal ADD Ordering BIGINT IDENTITY(1,1) NOT NULL;
ALTER TABLE EventJournal ADD CONSTRAINT PK_EventJournal PRIMARY KEY (Ordering);
ALTER TABLE EventJournal ADD CONSTRAINT QU_EventJournal UNIQUE (PersistenceID, SequenceNr);