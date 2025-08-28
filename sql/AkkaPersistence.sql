SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID(N'[dbo].[EventJournal]', N'U') IS NULL

CREATE TABLE [dbo].[EventJournal] (
  Ordering BIGINT IDENTITY(1,1) NOT NULL,
  PersistenceID NVARCHAR(255) NOT NULL,
  SequenceNr BIGINT NOT NULL,
  Timestamp BIGINT NOT NULL,
  IsDeleted BIT NOT NULL,
  Manifest NVARCHAR(500) NOT NULL,
  Payload VARBINARY(MAX) NOT NULL,
  Tags NVARCHAR(100) NULL,
  SerializerId INTEGER NULL
	CONSTRAINT PK_EventJournal PRIMARY KEY (Ordering),
  CONSTRAINT QU_EventJournal UNIQUE (PersistenceID, SequenceNr)
);

IF OBJECT_ID(N'[dbo].[SnapshotStore]', N'U') IS NULL

CREATE TABLE [dbo].[SnapshotStore] (
  PersistenceID NVARCHAR(255) NOT NULL,
  SequenceNr BIGINT NOT NULL,
  Timestamp DATETIME2 NOT NULL,
  Manifest NVARCHAR(500) NOT NULL,
  Snapshot VARBINARY(MAX) NOT NULL,
  SerializerId INTEGER NULL
  CONSTRAINT PK_SnapshotStore PRIMARY KEY (PersistenceID, SequenceNr)
);

IF OBJECT_ID(N'[dbo].[Metadata]', N'U') IS NULL

CREATE TABLE [dbo].[Metadata] (
  PersistenceID NVARCHAR(255) NOT NULL,
  SequenceNr BIGINT NOT NULL,
  CONSTRAINT PK_Metadata PRIMARY KEY (PersistenceID, SequenceNr)
);

IF OBJECT_ID(N'[dbo].[tags]', N'U') IS NULL

CREATE TABLE [dbo].[tags](
    [ordering_id] [bigint] NOT NULL,
    [tag] [nvarchar](64) NOT NULL,
    [sequence_nr] [bigint] NOT NULL,
    [persistence_id] [nvarchar](255) NOT NULL
    ) ON [PRIMARY]

IF OBJECT_ID(N'[dbo].[PK_tags]', N'PK') IS NULL

ALTER TABLE [dbo].[tags] ADD CONSTRAINT [PK_tags] PRIMARY KEY CLUSTERED
    (
    [ordering_id] ASC,
    [tag] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO