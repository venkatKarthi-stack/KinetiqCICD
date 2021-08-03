-- create stored proc to load detections full load
alter PROCEDURE usp_GetDetectionFullLoad @ClientID bigint
--delete from detection staging table 1 and load for a particular client id
AS
begin
--Since full load delete the entire table
truncate table Detections_Staging_1

--load detections into detections taging table 1
insert into Detections_Staging_1 (ClientHitID, ClientID, HitProcessingParametersId , AssetID, 
MediaID,  Country, MarketName, RegionName, Media, LocalStartDateTime, LocalStopDateTime, 
StartDateTimeUtc, StopDateTimeUtc, CreatedDateTime)
select 
ch.ClientHitID, c.ClientID, hpp.HitProcessingParametersId , a.AssetID, 
m.MediaID, m.Country, mt.MarketName, r.RegionName, m.Media, ch.LocalStartDateTime, 
ch.LocalStopDateTime, ch.StartDateTimeUtc, ch.StopDateTimeUtc, getdate() as CreatedDateTime
from IQTTX.dbo.Clients as c 
inner join IQTTX.dbo.HitProcessingParameters as hpp on c.ClientID = hpp.ClientId 
inner join IQTTX.civ.Assets a on a.HitProcessingParametersID = hpp.HitProcessingParametersId 
inner join IQTTX.civ.ClientHits as ch on a.AssetID = ch.AssetID
inner join IQTTX.dbo.media m on ch.MediaSourceID = m.MediaID 
inner join IQTTX.dbo.Markets as mt on m.MarketID = mt.MarketID 
inner join IQTTX.dbo.Regions as r on r.RegionId = mt.RegionId
where c.ClientID = @ClientID  and ch.visible = 1

-- to be removed
--update Detections_Staging_1 set country = marketname
end;



-- create stored proc to load assets full load
alter procedure usp_GetAssetsFullLoad @ClientID bigint
-- delete both assets table and insert into assets
as 
begin
--Since full load delete the entire table
truncate table assets_staging_1

delete from Assets_Master where ClientId = @ClientID

--load assets table into staging table 1
;with test as
(select
a.AssetID, c.ClientID, hpp.HitProcessingParametersId, 
mf.FieldName, mfv.InputValue, a.LastChangedDateTimeUtc
from IQTTX.dbo.Clients as c 
inner join IQTTX.dbo.HitProcessingParameters as hpp on c.ClientID = hpp.ClientId 
inner join IQTTX.civ.Assets a on a.HitProcessingParametersID = hpp.HitProcessingParametersId 
inner join IQTTX.civ.MetadataFieldValues mfv on mfv.AssetID = a.AssetID
inner join IQTTX.civ.MetadataFields mf on mfv.MetadataFieldID = mf.MetadataFieldID 
where c.ClientID = @ClientID --and mf.fieldname = 'approval_status' and mfv.inputvalue = 'approved'
)
insert into Assets_Staging_1 (AssetID, ClientID, HitProcessingParametersId , Sports, Disciplines, [Events], MedalCeremony, SourceStart, SourceStop, LastChangedDateTimeUtc, CreatedDateTime)
SELECT AssetID, ClientID, HitProcessingParametersId, [sports], [disciplines], [events], [medal_ceremony], [source_start], [source_stop], LastChangedDateTimeUtc, getdate() as CreatedDateTime
FROM
(
    SELECT *
    FROM test
) AS SourceTable 
PIVOT(max(InputValue) FOR [FieldName] IN([sports], [disciplines], [events], [medal_ceremony], [source_start], [source_stop])) AS PivotTable

--load all the Assets staging table 1 into Assets master table
insert into Assets_Master
select * from Assets_Staging_1
end;



-- create stored proc to join both tables
alter procedure usp_CombineDetectionsAndAssetsFullLoad @ClientID bigint 
--delete detections from the client id given and insert new detections from detection taging table 1 and assets master table
as
begin 
truncate table Detections_Staging_2
--delete detections for client id given
delete from Detections where ClientID = @ClientID
--insert into detections
insert into Detections_Staging_2 (ClientHitID, ClientID, AssetID, MediaID, Country, MarketName, RegionName, Channel, LocalStartdateTime, LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc,
Sports, Disciplines, [Events], MedalCeremony, AssetSourceStart, AssetSourceStop, IsPrime, IsLiveCalculated, CreatedDateTime)
select d.ClientHitID, d.ClientID, d.AssetID, d.MediaID, d.country, d.MarketName, d.RegionName, d.Media, d.LocalStartdateTime, d.LocalStopDateTime,d.StartDateTimeUtc, d.StopDateTimeUtc,
a.Sports, a.Disciplines, a.[Events], a.MedalCeremony, a.SourceStart, a.SourceStop, 'Non-Prime' as IsPrime, 'OTHER' as IsLive_Calculated, getdate() as CreatedDateTime
from detections_staging_1 d inner join Assets_Master a on d.Assetid = a.AssetID and d.ClientID = @ClientID

update [OlympicReporting].[dbo].Detections_Staging_2 set MedalCeremony = 'Non-Medal' where MedalCeremony = 'no' or MedalCeremony = ''

update [OlympicReporting].[dbo].Detections_Staging_2 set MedalCeremony = 'Medal' where MedalCeremony = 'yes'

update [OlympicReporting].[dbo].Detections_Staging_2 set IsPrime = 'Prime' where cast(LocalStartDateTime as time) > '19:00:00' and cast(LocalStartDateTime as time) < '22:59:00' 

update [OlympicReporting].[dbo].Detections_Staging_2 set IsLiveCalculated = 'LIVE' where StartDateTimeUtc > AssetSourceStart and  StopDateTimeUtc < DATEADD (ss, 5, AssetSourceStop)

insert into Detections
select * from Detections_Staging_2
end;


--detection spanning across 2 days
alter procedure usp_GetDetectionAcross2DaysFullLoad  @ClientID bigint
as
begin
truncate table Detections_Staging_3

insert into Detections_Staging_3
select ClientHitID , ClientID , AssetID , MediaID , Country,MarketName, RegionName,Channel, LocalStartdateTime, LocalStopDateTime, 
case
	when n = 1 then LocalStartDateTime
	when n = 2 then CAST( convert(date, LocalStopDateTime) AS DATETIME) + CAST( '00:00:00' AS DATETIME)
	else LocalStartDateTime
end as LocalStartDateTimeModified,
case
	when n = 1 then CAST( convert(date, LocalStartDateTime) AS DATETIME) + CAST( '23:59:59' AS DATETIME)
	when n = 2 then LocalStopDateTime
	else LocalStopDateTime
end as LocalStopDateTimeModified,
StartDateTimeUtc, StopDateTimeUtc,Sports, Disciplines,[Events], MedalCeremony,IsPrime ,IsLiveCalculated,AssetSourceStart,AssetSourceStop,CreatedDateTime,ModifiedDateTime
from
(select *
from Detections t1 cross join
     (select 1 as n union all select 2) n
	  where ClientID = @ClientID and convert(date, LocalStopDateTime) = dateadd(day, 1, CONVERT(date, LocalStartDateTime))) m
end;


--new min by min with channel and country mapping

alter procedure usp_GenerateMinByMinDetectionsWithAudience  @ClientID bigint
as
begin
--delete min by min staging table as well as final table
truncate table min_by_min_staging_data_model
delete from min_by_min_data_model  where ClientID =  @ClientID
--insert into min by min table with right join between audience master and detections table (right table is detections)

insert into min_by_min_staging_data_model (ClientID, ClientHitId, AssetId, Country, MarketName, RegionName, Channel, Sports, Disciplines, [Events], EventDatetime, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, DetectionSeconds, MedalCeremony, IsLiveCalculated, 
IsPrime, AverageAudience, TVRating, Share, EventDate)
select
k.clientId,k .ClientHitId, k.AssetId, k.Country, cy.MappingCountryName, k.RegionName, cl.MappingChannelName, sports, disciplines, events, k.EventTimeTable, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, 
case 
	when dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) = dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0) then Datediff(s, LocalStartDateTime, LocalStopDateTime)
	when LocalStopDateTime <  dateadd(mi, 1, EventDatetime) then Datediff(s, dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0), LocalStopDateTime)
	when  dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) =  EventDatetime then Datediff(s, LocalStartDateTime, dateadd(mi, 1, EventDatetime))
	else 60
 end as DetectionSeconds,
 MedalCeremony, IsLiveCalculated, IsPrime, AverageAudience, TVRating, Share, EventDate
 from
(select 
g.clientId, g.ClientHitId, g.AssetId, g.Country, g.MarketName, g.RegionName, g.Channel, sports, disciplines, events, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, 
MedalCeremony, IsLiveCalculated, IsPrime,
convert(datetime,  CONVERT(CHAR(8),CONVERT(date, LocalStartdatetime), 112) + ' ' + CONVERT(CHAR(8), t.EventTime, 108)) as EventTimeTable
from detections g
inner join Time_Master t on 
convert(time, dateadd(mi, datediff(mi, 0, g.LocalStartDateTime), 0)) <= t.EventTime and convert(time, dateadd(mi, datediff(mi, 0, g.LocalStopDateTime), 0)) >= t.EventTime) k
left join CountryMappingMaster cy on k.MarketName = cy.DetectionsCountryName
left join ChannelMappingMaster cl on k.Channel = cl.DetectionsChannelName
left join Audience_Master a on a.Territory = cy.AudienceCountryName and a.Channel = cl.AudienceChannelName and k.EventTimeTable = a.EventDatetime
where k.clientId = @ClientID and
convert(date, k.LocalStopDateTime) != dateadd(day, 1, CONVERT(date, k.LocalStartDateTime))

-- delete for those with 2 days 
delete from min_by_min_staging_data_model where ClientHitId in (select distinct ClientHitId from Detections_Staging_3)

-- for 2 days
insert into min_by_min_staging_data_model (ClientID, ClientHitId, AssetId, Country, MarketName, RegionName, Channel, Sports, Disciplines, [Events], EventDatetime, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, DetectionSeconds, MedalCeremony, IsLiveCalculated, 
IsPrime, AverageAudience, TVRating, Share, EventDate)
select
k.clientId,k .ClientHitId, k.AssetId, k.Country, cy.MappingCountryName, k.RegionName, cl.MappingChannelName, sports, disciplines, events, k.EventTimeTable, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, 
case 
	when dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) = dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0) then Datediff(s, LocalStartDateTime, LocalStopDateTime)
	when LocalStopDateTime <  dateadd(mi, 1, EventDatetime) then Datediff(s, dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0), LocalStopDateTime)
	when  dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) =  EventDatetime then Datediff(s, LocalStartDateTime, dateadd(mi, 1, EventDatetime))
	else 60
 end as DetectionSeconds,
 MedalCeremony, IsLiveCalculated, IsPrime, AverageAudience, TVRating, Share, EventDate
 from
(select 
g.clientId, g.ClientHitId, g.AssetId, g.Country, g.MarketName, g.RegionName, g.Channel, sports, disciplines, events, LocalStartDateTimeModified as LocalStartDateTime, 
LocalStopDateTimeModified as LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, 
MedalCeremony, IsLiveCalculated, IsPrime,
convert(datetime,  CONVERT(CHAR(8),CONVERT(date, LocalStartdatetimeModified), 112) + ' ' + CONVERT(CHAR(8), t.EventTime, 108)) as EventTimeTable
from Detections_Staging_3 g
inner join Time_Master t on 
convert(time, dateadd(mi, datediff(mi, 0, g.LocalStartDateTimeModified), 0)) <= t.EventTime and convert(time, dateadd(mi, datediff(mi, 0, g.LocalStopDateTimeModified), 0)) >= t.EventTime) k
left join CountryMappingMaster cy on k.MarketName = cy.DetectionsCountryName
left join ChannelMappingMaster cl on k.Channel = cl.DetectionsChannelName
left join Audience_Master a on a.Territory = cy.AudienceCountryName and a.Channel = cl.AudienceChannelName and k.EventTimeTable = a.EventDatetime
where k.clientId = @ClientID 

insert into min_by_min_data_model
select * from min_by_min_staging_data_model

-- delete those overlapping detections with less seconds in a minute
delete x from 
(select *, rn=row_number() over (partition by MarketName, Channel, EventDatetime order by EventDatetime, DetectionSeconds desc)
  from min_by_min_data_model where EventDatetime is not null) x
where rn > 1;
-- delete those detections where stopp time and event time is same because stop time with 0 milliseconds cannot be counted
delete from [OlympicReporting].[dbo].min_by_min_data_model where LocalStopDateTime = EventDatetime 
-- Load min by min staging table into min by min actual table

end;


--drop and rebuild index before calling SP before loading min by min
-- aggr sp's
alter procedure usp_GenerateAggrReports  @ClientID bigint
as
begin
-- aggregate Level 3
delete from AggrLevel3Event where ClientId = @ClientID

insert into AggrLevel3Event ([ClientID], MarketName, [Channel], [Sports], [Disciplines], [Events], [IsPrime], [IsLiveCalculated], [MedalCeremony], 
L3HoursSum, L3AudienceAvg, L3AudienceMax, L3ViewerHoursSum, L3HoursWithAudienceSum, L3ShareAVG)
select ClientID, MarketName, Channel, Sports, Disciplines, [Events], PrimeValue, LiveValue, MedalValue, 
	round(count(DetectionSeconds)/60.00, 2) as [L3HoursSum], round(avg(AverageAudience), 2) as L3AudienceAvg,
	max(AverageAudience) as L3AudienceMax, round(sum(AverageAudience)/60.00, 2) as L3ViewerHoursSum, 
	round((sum(AverageAudience)/ AVG(AverageAudience))/60.00, 2) as L3HoursWithAudienceSum , round(AVG(Share), 2) as L3ShareAVG
from min_by_min_data_model d 
inner join Prime_Master p on d.IsPrime = p.PrimeLink 
inner join Live_Master l on d.IsLiveCalculated = l.LiveLink 
inner join Medal_Master m on d.MedalCeremony = m.MedalLink 
where (channel = 'BBC 1 (London)' or channel = 'TV5 Philippines') and ClientID = @ClientID
group by ClientID, MarketName, Channel, Sports, Disciplines, [Events], PrimeValue, LiveValue, MedalValue

--level 2
delete from AggrLevel2Discipline where ClientId = @ClientID

insert into AggrLevel2Discipline ([ClientID], MarketName, [Channel], [Sports], [Disciplines], [IsPrime], [IsLiveCalculated], [MedalCeremony], 
L2HoursSum, L2AudienceAvg, L2AudienceMax, L2ViewerHoursSum, L2HoursWithAudienceSum, L2ShareAVG)
select ClientID, MarketName, Channel, Sports, Disciplines, PrimeValue, LiveValue, MedalValue, 
	round(count(DetectionSeconds)/60.00, 2) as [L3HoursSum], round(avg(AverageAudience), 2) as L3AudienceAvg,
	max(AverageAudience) as L3AudienceMax, round(sum(AverageAudience)/60.00, 2) as L3ViewerHoursSum, 
	round((sum(AverageAudience)/ AVG(AverageAudience))/60.00, 2) as L3HoursWithAudienceSum , round(AVG(Share), 2) as L3ShareAVG
from min_by_min_data_model d 
inner join Prime_Master p on d.IsPrime = p.PrimeLink 
inner join Live_Master l on d.IsLiveCalculated = l.LiveLink 
inner join Medal_Master m on d.MedalCeremony = m.MedalLink 
where (channel = 'BBC 1 (London)' or channel = 'TV5 Philippines') and ClientID = @ClientID
group by ClientID, MarketName, Channel, Sports, Disciplines, PrimeValue, LiveValue, MedalValue

--level 1
delete from AggrLevel1Sport where ClientId = @ClientID

insert into AggrLevel1Sport ([ClientID], MarketName, [Channel], [Sports], [IsPrime], [IsLiveCalculated], [MedalCeremony], 
L1HoursSum, L1AudienceAvg, L1AudienceMax, L1ViewerHoursSum, L1HoursWithAudienceSum, L1ShareAVG)
select ClientID, MarketName, Channel, Sports, PrimeValue, LiveValue, MedalValue, 
	round(count(DetectionSeconds)/60.00, 2) as [L3HoursSum], round(avg(AverageAudience), 2) as L3AudienceAvg,
	max(AverageAudience) as L3AudienceMax, round(sum(AverageAudience)/60.00, 2) as L3ViewerHoursSum, 
	round((sum(AverageAudience)/ AVG(AverageAudience))/60.00, 2) as L3HoursWithAudienceSum , round(AVG(Share), 2) as L3ShareAVG
from min_by_min_data_model d 
inner join Prime_Master p on d.IsPrime = p.PrimeLink 
inner join Live_Master l on d.IsLiveCalculated = l.LiveLink 
inner join Medal_Master m on d.MedalCeremony = m.MedalLink 
where (channel = 'BBC 1 (London)' or channel = 'TV5 Philippines') and ClientID = @ClientID
group by ClientID, MarketName, Channel, Sports, PrimeValue, LiveValue, MedalValue

--level 0 
delete from AggrLevel0Channel where ClientId = @ClientID

insert into AggrLevel0Channel ([ClientID], MarketName, [Channel], [IsPrime], [IsLiveCalculated], [MedalCeremony], 
L0HoursSum, L0AudienceAvg, L0AudienceMax, L0ViewerHoursSum, L0HoursWithAudienceSum, L0ShareAVG)
select ClientID, MarketName, Channel, PrimeValue, LiveValue, MedalValue, 
	round(count(DetectionSeconds)/60.00, 2) as [L3HoursSum], round(avg(AverageAudience), 2) as L3AudienceAvg,
	max(AverageAudience) as L3AudienceMax, round(sum(AverageAudience)/60.00, 2) as L3ViewerHoursSum, 
	round((sum(AverageAudience)/ AVG(AverageAudience))/60.00, 2) as L3HoursWithAudienceSum , round(AVG(Share), 2) as L3ShareAVG
from min_by_min_data_model d 
inner join Prime_Master p on d.IsPrime = p.PrimeLink 
inner join Live_Master l on d.IsLiveCalculated = l.LiveLink 
inner join Medal_Master m on d.MedalCeremony = m.MedalLink 
where (channel = 'BBC 1 (London)' or channel = 'TV5 Philippines') and ClientID = @ClientID
group by ClientID, MarketName, Channel, PrimeValue, LiveValue, MedalValue
end;


--full load
exec usp_GetDetectionFullLoad @ClientID = '17069' 
go
exec usp_GetAssetsFullLoad @ClientID = '17069' 
go
exec usp_CombineDetectionsAndAssetsFullLoad @ClientID = '17069' 
go
exec usp_GetDetectionAcross2DaysFullLoad @ClientID = '17069' 
go
exec usp_GenerateMinByMinDetectionsWithAudience  @ClientID = '17069' 
go
exec usp_GenerateAggrReports  @ClientID = '17069' 
































/*latest old min by min
select 
k.clientId,k .ClientHitId, k.AssetId, k.Country, k.MarketName, k.RegionName, k.Channel, sports, disciplines, events, k.EventTimeTable, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, 
case 
	when dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) = dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0) then Datediff(s, LocalStartDateTime, LocalStopDateTime)
	when LocalStopDateTime <  dateadd(mi, 1, EventDatetime) then Datediff(s, dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0), LocalStopDateTime)
	when  dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) =  EventDatetime then Datediff(s, LocalStartDateTime, dateadd(mi, 1, EventDatetime))
	else 60
 end as DetectionSeconds,
 MedalCeremony, IsLiveCalculated, IsPrime, AverageAudience, TVRating, Share, EventDate
 from
(select 
g.clientId, g.ClientHitId, g.AssetId, g.Country, g.MarketName, g.RegionName, g.Channel, sports, disciplines, events, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, 
MedalCeremony, IsLiveCalculated, IsPrime,
convert(datetime,  CONVERT(CHAR(8),CONVERT(date, LocalStartdatetime), 112) + ' ' + CONVERT(CHAR(8), t.EventTime, 108)) as EventTimeTable
from detections g
inner join Time_Master t on 
convert(time, dateadd(mi, datediff(mi, 0, g.LocalStartDateTime), 0)) <= t.EventTime and convert(time, dateadd(mi, datediff(mi, 0, g.LocalStopDateTime), 0)) >= t.EventTime) k
left join 
Audience_Master a 
on a.Territory = k.MarketName and a.Channel = k.Channel
and k.EventTimeTable = a.EventDatetime
where k.clientId = @ClientID
*/








--min by min sp
/*
alter procedure usp_GenerateMinByMinDetectionsWithAudience  @ClientID bigint
as
begin
--delete min by min staging table as well as final table
truncate table min_by_min_staging_data_model
delete from min_by_min_data_model  where ClientID = @ClientID
--insert into min by min table with right join between audience master and detections table (right table is detections)
insert into min_by_min_staging_data_model (ClientID, ClientHitId, AssetId, Country, MarketName, RegionName, Channel, Sports, Disciplines, [Events], EventDatetime, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, ViewerSeconds, MedalCeremony, IsLiveCalculated, 
IsPrime, AverageAudience, TVRating, Share, EventDate)
select b.clientId, b.ClientHitId, b.AssetId, b.Country, b.MarketName, b.RegionName, b.Channel, sports, disciplines, events, EventDatetime, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, 
 case 
	when dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) = dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0) then Datediff(s, LocalStartDateTime, LocalStopDateTime)
	when LocalStopDateTime <  dateadd(mi, 1, EventDatetime) then Datediff(s, dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0), LocalStopDateTime)
	when  dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) =  EventDatetime then Datediff(s, LocalStartDateTime, dateadd(mi, 1, EventDatetime))
	when EventDatetime is null then Datediff(s, LocalStartDateTime, LocalStopDateTime)
	else 60
 end as ViewerSeconds,
MedalCeremony, IsLiveCalculated, IsPrime, AverageAudience, TVRating, Share, EventDate
 from Audience_Master a right join detections b on a.Territory = b.Country and a.Channel = b.Channel
and  dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) <= a.EventDatetime and 
   dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0) >= a.EventDatetime where b.clientId = @ClientID 

update min_by_min_staging_data_model set ViewerSecondsWithAudience = ViewerSeconds where AverageAudience is not null and  EventDatetime is not null

insert into min_by_min_data_model
select * from min_by_min_staging_data_model

-- delete those overlapping detections with less seconds in a minute
delete x from 
(select *, rn=row_number() over (partition by Country, Channel, EventDatetime order by EventDatetime, ViewerSeconds desc)
  from min_by_min_data_model where EventDatetime is not null) x
where rn > 1;
-- delete those detections where stopp time and event time is same because stop time with 0 milliseconds cannot be counted
delete from [OlympicReporting].[dbo].min_by_min_data_model where LocalStopDateTime = EventDatetime 
-- Load min by min staging table into min by min actual table

end;
*/
/*
--------------new min by min query
alter procedure usp_GenerateMinByMinDetectionsWithAudience  @ClientID bigint
as
begin
--delete min by min staging table as well as final table
truncate table min_by_min_staging_data_model
delete from min_by_min_data_model  where ClientID =  @ClientID
--insert into min by min table with right join between audience master and detections table (right table is detections)

insert into min_by_min_staging_data_model (ClientID, ClientHitId, AssetId, Country, MarketName, RegionName, Channel, Sports, Disciplines, [Events], EventDatetime, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, DetectionSeconds, MedalCeremony, IsLiveCalculated, 
IsPrime, AverageAudience, TVRating, Share, EventDate)
select 
g.clientId, g.ClientHitId, g.AssetId, g.Country, g.MarketName, g.RegionName, g.Channel, sports, disciplines, events, g.EventDatetime, LocalStartDateTime, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, 
case 
	when dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) = dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0) then Datediff(s, LocalStartDateTime, LocalStopDateTime)
	when LocalStopDateTime <  dateadd(mi, 1, EventDatetime) then Datediff(s, dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0), LocalStopDateTime)
	when  dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) =  EventDatetime then Datediff(s, LocalStartDateTime, dateadd(mi, 1, EventDatetime))
	else 60
 end as DetectionSeconds, 
MedalCeremony, IsLiveCalculated, IsPrime, AverageAudience, TVRating, Share, EventDate
from 
(
select 
b.clientId, b.ClientHitId, b.AssetId, b.Country, b.MarketName, b.RegionName, b.Channel, sports, disciplines, events, 
LocalStopDateTime, StartDateTimeUtc, StopDateTimeUtc, AssetSourceStart, AssetSourceStop, 
MedalCeremony, IsLiveCalculated, IsPrime, AverageAudience, TVRating, Share, EventDate, LocalStartDateTime,
case 
	when EventDatetime is not null then EventDatetime
	else  convert(datetime,  CONVERT(CHAR(8),CONVERT(date, LocalStartdatetime), 112) + ' ' + CONVERT(CHAR(8), t.EventTime, 108))
end as EventDatetime
from Audience_Master a right join detections b on a.Territory = b.Country and a.Channel = b.Channel
and  dateadd(mi, datediff(mi, 0, LocalStartDateTime), 0) <= a.EventDatetime and 
dateadd(mi, datediff(mi, 0, LocalStopDateTime), 0) >= a.EventDatetime 
inner join Time_Master t on 
convert(time, dateadd(mi, datediff(mi, 0, b.LocalStartDateTime), 0)) <= t.EventTime and convert(time, dateadd(mi, datediff(mi, 0, b.LocalStopDateTime), 0)) >= t.EventTime
where b.clientId = @ClientID) g

insert into min_by_min_data_model
select * from min_by_min_staging_data_model

-- delete those overlapping detections with less seconds in a minute
delete x from 
(select *, rn=row_number() over (partition by Country, Channel, EventDatetime order by EventDatetime, DetectionSeconds desc)
  from min_by_min_data_model where EventDatetime is not null) x
where rn > 1;
-- delete those detections where stopp time and event time is same because stop time with 0 milliseconds cannot be counted
delete from [OlympicReporting].[dbo].min_by_min_data_model where LocalStopDateTime = EventDatetime 
-- Load min by min staging table into min by min actual table

end;

*/








