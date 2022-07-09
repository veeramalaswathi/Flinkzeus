package com.flink.basics;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZeusRawEventsMapper implements MapFunction<Tuple1<String>, ZeusEvent> {

    private static final Logger log = LoggerFactory.getLogger(ZeusRawEventsMapper.class);

    @Override
    public ZeusEvent map(Tuple1<String> value) throws Exception {
        ObjectMapper jsonParser = new ObjectMapper();
        // log.info("Input JSON String = {}",value.f0);
        JsonNode node = jsonParser.readValue(value.f0, JsonNode.class);
        return createZeusEvent(node);
    }

    private ZeusEvent createZeusEvent(JsonNode node) {

        ZeusEvent event = new ZeusEvent();

        event.setEventVersion(getString(node, "Event_Version", null));
        event.setEventSequenceID(getInteger(node, "Event_SequenceID"));
        event.setEventTypeID(getInteger(node, "Event_EventTypeID"));
        event.setEventTypeName(getString(node, "Event_EventTypeName", null));
        event.setEventSessionID(getString(node, "Event_SessionID", null));
        event.setPlaybackSessionID(getString(node, "Event_PlaybackSessionID", null));
        event.setAuthenticationToken(getString(node, "Event_AuthenticationToken", null));
        event.setStageID(getString(node, "Event_StageID", null));
        event.setAssetTime(getLong(node, "Event_TimeInfo_AssetTime"));
        event.setContentTime(getLong(node, "Event_TimeInfo_ContentTime"));
        event.setLive(getBoolean(node, "Event_TimeInfo_IsLive"));
        event.setAtLive(getBoolean(node, "Event_TimeInfo_IsAtLive"));
        event.setOriginalAssetTime(getLong(node, "Event_TimeInfo_OriginalAssetTime"));
        event.setUpdatedAssetTime(getLong(node, "Event_TimeInfo_UpdatedAssetTime"));
        event.setContentType(getString(node, "Event_TimeInfo_ContentType", null));
        event.setContentTypeID(getString(node, "Event_TimeInfo_ContentTypeID", null));
        event.setEventTime(getLong(node, "Event_TimeInfo_UTCTime"));
        event.setApTimeSet(getLong(node, "Event_TimeInfo_APTimeSet"));


        event.setAssetAnchorTime(getLong(node, "Event_Fields_AssetAnchorTime"));

        //Event_Fields_AssetCallSign or channel_call_sign
        event.setAssetCallSign(getString(node, "Event_Fields_AssetCallSign", "channel_call_sign"));

        //Event_Fields_AssetChannelGenre or channel_genre
        event.setAssetChannelGenre(getString(node, "Event_Fields_AssetChannelGenre", "channel_genre"));

        //cms_channel_guid or Event_Fields_AssetChannelGuid
        event.setAssetChannelGuid(getString(node, "Event_Fields_AssetChannelGuid", "cms_channel_guid"));

        //channel_name - Event_Fields_AssetChannelName is showing call sign only.
        event.setAssetChannelName(getString(node, "channel_name", "Event_Fields_AssetChannelName"));

        //AssetContentType - check in Athena - only assetcontentType is good
        event.setAssetContentType(getString(node, "Event_Fields_AssetContentType", null).toLowerCase());

        //In VideoHeartBeat or AssetStarted
        event.setAssetDuration(getLong(node, "Event_Fields_AssetDuration"));


        event.setAssetEpisodeName(getString(node, "Event_Fields_AssetEpisodeName", null));
        event.setAssetEpisodeNumber(getLong(node, "Event_Fields_AssetEpisodeNumber"));
        event.setAssetFranchiseGuid(getString(node, "Event_Fields_AssetFranchiseGuid", null));
        event.setAssetFranchiseTitle(getString(node, "Event_Fields_AssetFranchiseTitle", null));

        //VideoHeartbeat or AssetStarted
        event.setAssetGenre(getString(node, "Event_Fields_AssetGenre", null));

        event.setAssetGuid(getString(node, "Event_Fields_AssetGuid", null));

        event.setAssetID3Tagged(getString(node, "Event_Fields_AssetID3Tagged", null));

        event.setAssetInfoUrl(getString(node, "Event_Fields_AssetInfoUrl", null));
        event.setAssetIsLinear(getString(node, "Event_Fields_AssetIsLinear", null));
        event.setAssetIsLive(getString(node, "Event_Fields_AssetIsLive", null));
        //VideoHeartBeat or AssetStarted
        event.setAssetProgramGuid(getString(node, "Event_Fields_AssetProgramGuid", null));
        event.setAssetProgramType(getString(node, "Event_Fields_AssetProgramType", null));

        //VideoHeartBeat Playing state or AssetStarted
        event.setAssetPublisherAssetID(getString(node, "Event_Fields_AssetPublisherAssetID", null));

        event.setAssetRating(getString(node, "Event_Fields_AssetRating", null));
        //VHB or AssetStarted boolean
        event.setAssetSVOD(getString(node, "Event_Fields_AssetSVOD", null));

        //Event_Fields_AssetTitle if not exists then asset_title
        event.setAssetTitle(getString(node, "Event_Fields_AssetTitle", null));
        //VHB or AssetStarted - Event_Fields_AssetURL or asset_info_url
        event.setAssetURL(getString(node, "Event_Fields_AssetURL", null));

        event.setAssetUsingDynamicAds(getString(node, "Event_Fields_AssetUsingDynamicAds", null));

        //Correct
        event.setBitrate(getDouble(node, "Event_Fields_Bitrate"));

        event.setCdn(getString(node, "Event_Fields_CDN", null));
        event.setCdnName(getString(node, "Event_Fields_CDNName", null));

        //Event_Fields_Duration has the value of buffering duration for a given BufferingStart and BufferingEnd event
        // Used for computing seekBufferingTime (Restart Time)
        event.setDuration(getLong(node, "Event_Fields_Duration"));

        //Difference between AssetIsLive and IsLive ??? - conflicting values
        event.setLive(getBoolean(node, "Event_Fields_IsLive"));

        //Check for empty
        event.setItemId(getString(node, "Event_Fields_ItemId", null));

        event.setPlayType(getString(node, "Event_Fields_PlayType", null));

        //AssetEnded, AssetStarted, VHB Sometimes - not consistent
        event.setQueryId(getString(node, "Event_Fields_QueryId", null));

        //> 0
        event.setTotalVideoPlaytime(getLong(node, "Event_Fields_TotalVideoPlaytime"));
        // Waste Field
        event.setVersion(getString(node, "Version", null));

        //Version
        event.setPlatformID(getString(node, "PlatformID", null));

        //
        event.setDeviceType(getString(node, "DeviceInformation_DeviceType", null));
        event.setDeviceVendor(getString(node, "DeviceInformation_DeviceVendor", null));
        event.setDeviceModel(getString(node, "DeviceInformation_DeviceModel", null));
        event.setDeviceName(getString(node, "DeviceInformation_DeviceName", null));
        event.setUniqueDeviceID(getString(node, "DeviceInformation_UniqueDeviceID", null));


        event.setCity(getString(node, "GeoInformation_City", null));
        event.setCountry(getString(node, "GeoInformation_Country", null));
        event.setDMA(getString(node, "GeoInformation_Dma", null));
        event.setRegion(getString(node, "GeoInformation_Region", null));
        event.setZipCode(getString(node, "GeoInformation_ZipCode", null));
        event.setLatitude(getString(node, "GeoInformation_Latitude", null));
        event.setLongitude(getString(node, "GeoInformation_Longitude", null));
        event.setISPInformation(getString(node, "GeoInformation_ISPInformation", null));
        event.setIPAddress(getString(node, "GeoInformation_IPAddress", null));
        event.setTimeZoneOffset(getString(node, "GeoInformation_TimeZoneOffset", null));

//Add SoftwareInfomation_CSLName to identify APJS ???
        event.setService(getString(node, "SoftwareInformation_Service", null));
        event.setOSVersion(getString(node, "SoftwareInformation_OSVersion", null));
        event.setOSType(getString(node, "SoftwareInformation_OSType", null));
        event.setCSLVersion(getString(node, "SoftwareInformation_CSLVersion", null));
        event.setAppVersion(getString(node, "SoftwareInformation_AppVersion", null));
        event.setPlayerFrameworkName(getString(node, "SoftwareInformation_PlayerFrameworkName", null));
        event.setPlayerFrameworkVersion(getString(node, "SoftwareInformation_PlayerFrameworkVersion", null));
        event.setDebugBuild(getBoolean(node, "SoftwareInformation_DebugBuild"));
        event.setSupportMode(getBoolean(node, "SoftwareInformation_SupportMode"));


        event.setxService(getString(node, "XService", null));

        event.setReceivedAtInUtcMs(getLong(node, "ReceivedAtInUtcMs"));

        // Waste Use GeoInformation IP Address - RemoteIPAddress is our gateway address
        event.setRemoteIpAddress(getString(node, "RemoteIpAddress", null));

        event.setReceivedAt(getString(node, "ReceivedAt", null));
        event.setReceivedAtInUtcSeconds(getLong(node, "ReceivedAtInUtcSeconds"));

        //WAste Field
        event.setSource(getString(node, "Source", null));

        event.setUserId(getString(node, "id_sub", null));

        event.setPost_time_secs(getLong(node, "post_time_secs"));
        event.setPost_time_text(getString(node, "post_time_text", null));
        event.setPost_time_date(getString(node, "post_time_date", null));
        event.setPost_time_hour(getInteger(node, "post_time_hour"));

        event.setPlatform(getString(node, "Platform", null));

        //Check null
        event.setContainerId(getString(node, "container_id", null));
        event.setAvailabilityType(getString(node, "availability_type", null));
        event.setSessionOrigin(getString(node, "Event_Fields_SessionOrigin", null));
        //check null programName - there is no AssetProgramName
        event.setProgramName(getString(node, "program_name", null));

        // complement with assetProgramType
        event.setProgramType(getString(node, "program_type", null));

        //AssetStarted only
        event.setStartupDuration(getLong(node, "Event_Fields_StartupDuration"));

        //Check Data Quality
        event.setTotalBufferTime(getLong(node, "Event_Fields_TotalBufferTime"));

        //EventType is AppError
        event.setErrorTypeName(getString(node, "Event_Fields_ErrorTypeName", null));
        event.setErrorMessage(getString(node, "Event_Fields_ErrorMessage", null));
        event.setErrorPath(getString(node, "Event_Fields_Details_ErrorPath", "Event_Fields_ErrorPath"));
        event.setSeverityId(getInteger(node, "Event_Fields_SeverityID"));
        event.setHttpStatusCode(getInteger(node, "Event_Fields_Details_HttpStatusCode"));

        //EventType is PlaybackSessionClosed
        event.setClosedType(getString(node, "Event_Fields_ClosedType", null));

        //EventType is AppError
        event.setSeverityName(getString(node, "Event_Fields_SeverityName", null));
        //EventType is VideoHeartBeat
        event.setPlayerState(getString(node, "Event_Fields_PlayerState", null));

        //BufferingStart and End - Data Quality
        event.setSeekInducedBuffering(getBoolean(node, "Event_Fields_SeekInducedBuffering"));

        //Waste of QoS
        event.setFlink_kafka_event_write_ts(getLong(node, "flink_kafka_event_write_ts"));

        // New Fields
        event.setPublisher(getString(node, "event_fields_publisher", null));
        event.setSeasonNumber(getString(node, "Event_Fields_AssetSeasonNumber", "Event_Fields_SeasonNumber"));
        event.setCdnHost(getString(node, "Event_Fields_CDN_Host", null));
        event.setNetworkType(getString(node, "Event_Fields_NetworkType", null));
        event.setSubscriptionPacks(getString(node, "Event_Fields_SubscriptionPacks", null));
        event.setDeviceModelYear(getString(node, "DeviceInformation_DeviceModelYear", null));
        event.setDeviceDisplayMode(getString(node, "DeviceInformation_DisplayMode", null));
        event.setSoftwareBrowserName(getString(node, "SoftwareInformation_BrowserName", null));
        event.setSoftwareBrowserVersion(getString(node, "SoftwareInformation_BrowserVersion", null));
        event.setWaitTime(getLong(node,"Event_Fields_WaitTime"));

        event.setInternalPlayerErrorCode(getString(node, "Event_Fields_internalPlayerErrorCode", null));
        event.setDrm(getString(node, "Event_Fields_DRM", null));
        event.setProtocol(getString(node, "Event_Fields_Protocol", null));
        event.setUserAgent(getString(node, "DeviceInformation_UserAgent", null));
        event.setChipset(getString(node, "DeviceInformation_Chipset", null));
        event.setTunerType(getString(node, "DeviceInformation_TunerType", null));
        event.setPackageName(getString(node, "Event_Fields_PackageName", null));
        event.setOta(getBoolean(node, "Event_Fields_isOta"));

        String adInsertType = getString(node, "Event_Fields_AdInsertType", null);
        event.setAdInsertType(StringUtils.isEmpty(adInsertType) ? "CSAI" : adInsertType);

        event.setEventsCount(getLong(node,"EventsCount"));
        return event;
    }

    private static final String NOT_AVAILABLE = "NA";

    private String getString(JsonNode node, String field, String alternateField) {
        if (node.hasNonNull(field)) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode != null)
                return fieldNode.asText();
        } else if (node.hasNonNull(alternateField)) {
            JsonNode fieldNode = node.get(alternateField);
            if (fieldNode != null)
                return fieldNode.asText();
        }
        return NOT_AVAILABLE;
    }

    private Long getLong(JsonNode node, String field) {
        if (node.hasNonNull(field)) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode != null)
                return fieldNode.asLong();
        } else {
            return -1L;
        }
        return -1L;
    }

    private Double getDouble(JsonNode node, String field) {
        if (node.hasNonNull(field)) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode != null)
                return fieldNode.asDouble();
        } else {
            return 0.0;
        }
        return 0.0;
    }


    private Integer getInteger(JsonNode node, String field) {
        if (node.hasNonNull(field)) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode != null)
                return fieldNode.asInt();
        } else {
            return -1;
        }
        return -1;
    }

    private Boolean getBoolean(JsonNode node, String field) {
        if (node.hasNonNull(field)) {
            JsonNode fieldNode = node.get(field);
            if (fieldNode != null)
                return fieldNode.asBoolean();
        } else {
            return false;
        }
        return false;
    }


}
