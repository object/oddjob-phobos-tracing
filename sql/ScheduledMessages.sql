SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID(N'[dbo].[ScheduledMessages]', N'U') IS NULL

CREATE TABLE [dbo].[ScheduledMessages](
	[ScheduledMessageId] [bigint] IDENTITY(1,1) NOT NULL,
	[MessageReference] [nvarchar](255) NOT NULL,
	[RecipientCategory] [nvarchar](255) NOT NULL,
	[RecipientPath] [nvarchar](255) NOT NULL,
	[Message] [nvarchar](max) NOT NULL,
	[MessageVersion] [int] NOT NULL,
	[TriggerTime] [datetime] NOT NULL,
	[Triggered] [bit] NOT NULL,
	[Cancelled] [bit] NOT NULL,
 CONSTRAINT [PK_ScheduledMessages] PRIMARY KEY NONCLUSTERED 
(
	[ScheduledMessageId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

CREATE CLUSTERED INDEX [IX_ScheduledMessages_TriggerTime] ON [dbo].[ScheduledMessages]
(
	[TriggerTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

CREATE NONCLUSTERED INDEX [IX_ScheduledMessage_Reference] ON [dbo].[ScheduledMessages]
(
	[MessageReference] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

GO
