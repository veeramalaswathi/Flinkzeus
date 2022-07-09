package com.flink.basics;


import java.io.Serializable;


public class ZeusEvent implements Serializable {

    private String eventVersion;
    private Integer eventSequenceID;
    private Integer eventTypeID;
    private String eventTypeName;
    private String eventSessionID;
    private String playbackSessionID;
    private String authenticationToken;
    private String sessionOrigin;
    private String stageID;
    private Long assetTime;
    private Long contentTime;
    private Boolean live;
    private Boolean atLive;
    private Long originalAssetTime;
    private Long updatedAssetTime;
    private String contentType;
    private String contentTypeID;
    private Long eventTime;
    private Long apTimeSet;
    private Long assetAnchorTime;
    private String assetCallSign;
    private String assetChannelGenre;
    private String assetChannelGuid;
    private String assetChannelName;
    private String assetContentType;
    private Long assetDuration;
    private String assetEpisodeName;
    private Long assetEpisodeNumber;
    private String assetFranchiseGuid;
    private String assetFranchiseTitle;
    private String assetGenre;
    private String assetGuid;
    private String assetID3Tagged;
    private String assetInfoUrl;
    private String assetIsLinear;
    private String assetIsLive;
    private String assetProgramGuid;
    private String assetProgramType;
    private String assetPublisherAssetID;
    private String assetRating;
    private String assetSVOD;
    private String assetTitle;
    private String assetURL;
    private String assetUsingDynamicAds;
    private Double bitrate;
    private String cdn;
    private Integer clipCount;
    private Long duration;
    private String itemId;
    private String playType;
    private String queryId;
    private Long totalVideoPlaytime;
    private String version;
    private String platformID;
    private String deviceType;
    private String deviceVendor;
    private String deviceModel;
    private String deviceName;
    private String uniqueDeviceID;
    private String city;
    private String country;
    private String DMA;
    private String region;
    private String zipCode;
    private String latitude;
    private String longitude;
    private String ISPInformation;
    private String IPAddress;
    private String TimeZoneOffset;
    private String service;
    private String OSVersion;
    private String OSType;
    private String CSLVersion;
    private String AppVersion;
    private String PlayerFrameworkName;
    private String PlayerFrameworkVersion;
    private Boolean DebugBuild;
    private Boolean SupportMode;
    private String encryptionType;
    private String hashValue;
    private String hashType;
    private String xSupportMode;
    private String xDeviceId;
    private String xService;
    private Long receivedAtInUtcMs;
    private String openrestyRequestId;
    private String requestLargePayloadAllowed;
    private String requestBodySize;
    private String remoteClientIpAddress;
    private String remoteIpAddress;
    private String kafkaRequestKey;
    private String receivedAt;
    private Long receivedAtInUtcSeconds;
    private String source;
    private String userId;
    private Long post_time_secs;
    private String post_time_text;
    private String post_time_date;
    private Integer post_time_hour;
    private String platform;
    private String cdnName;
    private String containerId;
    private String cmsChannelGuid;
    private String availabilityType;
    private String programName;
    private String programType;
    private Long startupDuration;
    private Long totalBufferTime;
    private String errorPath;
    private String errorTypeName;
    private String errorMessage;
    private String closedType;
    private String severityName;
    private Integer severityId;
    private String playerState;
    private Boolean seekInducedBuffering;
    private Long flink_kafka_event_write_ts;

    private String publisher;
    private String seasonNumber;
    private String cdnHost;
    private String networkType;
    private String subscriptionPacks;
    private String deviceModelYear;
    private String deviceDisplayMode;
    private String softwareBrowserName;
    private String softwareBrowserVersion;
    private Long waitTime;
    private Integer httpStatusCode;

    private String internalPlayerErrorCode;
    private String drm;
    private String protocol;
    private String userAgent;
    private String chipset;
    private String tunerType;
    private String packageName;
    private Boolean ota;
    private String adInsertType;

    public Integer getHttpStatusCode() { return httpStatusCode; }

    public void setHttpStatusCode(Integer httpStatusCode) { this.httpStatusCode = httpStatusCode; }

    private Long eventsCount;

    public Long getEventsCount() {
        return eventsCount;
    }

    public void setEventsCount(Long eventsCount) {
        this.eventsCount = eventsCount;
    }

    public ZeusEvent() {
        waitTime = 0L;
    }

    public String getEventVersion() {
        return eventVersion;
    }

    public void setEventVersion(String eventVersion) {
        this.eventVersion = eventVersion;
    }

    public Integer getEventSequenceID() {
        return eventSequenceID;
    }

    public void setEventSequenceID(Integer eventSequenceID) {
        this.eventSequenceID = eventSequenceID;
    }

    public Integer getEventTypeID() {
        return eventTypeID;
    }

    public void setEventTypeID(Integer eventTypeID) {
        this.eventTypeID = eventTypeID;
    }

    public String getEventTypeName() {
        return eventTypeName;
    }

    public void setEventTypeName(String eventTypeName) {
        this.eventTypeName = eventTypeName;
    }

    public String getEventSessionID() {
        return eventSessionID;
    }

    public void setEventSessionID(String eventSessionID) {
        this.eventSessionID = eventSessionID;
    }

    public String getPlaybackSessionID() {
        return playbackSessionID;
    }

    public void setPlaybackSessionID(String playbackSessionID) {
        this.playbackSessionID = playbackSessionID;
    }

    public String getAuthenticationToken() {
        return authenticationToken;
    }

    public void setAuthenticationToken(String authenticationToken) {
        this.authenticationToken = authenticationToken;
    }

    public String getSessionOrigin() {
        return sessionOrigin;
    }

    public void setSessionOrigin(String sessionOrigin) {
        this.sessionOrigin = sessionOrigin;
    }

    public String getStageID() {
        return stageID;
    }

    public void setStageID(String stageID) {
        this.stageID = stageID;
    }

    public Long getAssetTime() {
        return assetTime;
    }

    public void setAssetTime(Long assetTime) {
        this.assetTime = assetTime;
    }

    public Long getContentTime() {
        return contentTime;
    }

    public void setContentTime(Long contentTime) {
        this.contentTime = contentTime;
    }

    public Boolean getLive() {
        return live;
    }

    public void setLive(Boolean live) {
        this.live = live;
    }

    public Boolean getAtLive() {
        return atLive;
    }

    public void setAtLive(Boolean atLive) {
        this.atLive = atLive;
    }

    public Long getOriginalAssetTime() {
        return originalAssetTime;
    }

    public void setOriginalAssetTime(Long originalAssetTime) {
        this.originalAssetTime = originalAssetTime;
    }

    public Long getUpdatedAssetTime() {
        return updatedAssetTime;
    }

    public void setUpdatedAssetTime(Long updatedAssetTime) {
        this.updatedAssetTime = updatedAssetTime;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getContentTypeID() {
        return contentTypeID;
    }

    public void setContentTypeID(String contentTypeID) {
        this.contentTypeID = contentTypeID;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public Long getApTimeSet() {
        return apTimeSet;
    }

    public void setApTimeSet(Long apTimeSet) {
        this.apTimeSet = apTimeSet;
    }

    public Long getAssetAnchorTime() {
        return assetAnchorTime;
    }

    public void setAssetAnchorTime(Long assetAnchorTime) {
        this.assetAnchorTime = assetAnchorTime;
    }

    public String getAssetCallSign() {
        return assetCallSign;
    }

    public void setAssetCallSign(String assetCallSign) {
        this.assetCallSign = assetCallSign;
    }

    public String getAssetChannelGenre() {
        return assetChannelGenre;
    }

    public void setAssetChannelGenre(String assetChannelGenre) {
        this.assetChannelGenre = assetChannelGenre;
    }

    public String getAssetChannelGuid() {
        return assetChannelGuid;
    }

    public void setAssetChannelGuid(String assetChannelGuid) {
        this.assetChannelGuid = assetChannelGuid;
    }

    public String getAssetChannelName() {
        return assetChannelName;
    }

    public void setAssetChannelName(String assetChannelName) {
        this.assetChannelName = assetChannelName;
    }

    public String getAssetContentType() {
        return assetContentType;
    }

    public void setAssetContentType(String assetContentType) {
        this.assetContentType = assetContentType;
    }

    public Long getAssetDuration() {
        return assetDuration;
    }

    public void setAssetDuration(Long assetDuration) {
        this.assetDuration = assetDuration;
    }

    public String getAssetEpisodeName() {
        return assetEpisodeName;
    }

    public void setAssetEpisodeName(String assetEpisodeName) {
        this.assetEpisodeName = assetEpisodeName;
    }

    public Long getAssetEpisodeNumber() { return assetEpisodeNumber; }

    public void setAssetEpisodeNumber(Long assetEpisodeNumber) { this.assetEpisodeNumber = assetEpisodeNumber; }

    public String getAssetFranchiseGuid() {
        return assetFranchiseGuid;
    }

    public void setAssetFranchiseGuid(String assetFranchiseGuid) {
        this.assetFranchiseGuid = assetFranchiseGuid;
    }

    public String getAssetFranchiseTitle() {
        return assetFranchiseTitle;
    }

    public void setAssetFranchiseTitle(String assetFranchiseTitle) {
        this.assetFranchiseTitle = assetFranchiseTitle;
    }

    public String getAssetGenre() {
        return assetGenre;
    }

    public void setAssetGenre(String assetGenre) {
        this.assetGenre = assetGenre;
    }

    public String getAssetGuid() {
        return assetGuid;
    }

    public void setAssetGuid(String assetGuid) {
        this.assetGuid = assetGuid;
    }

    public String getAssetID3Tagged() {
        return assetID3Tagged;
    }

    public void setAssetID3Tagged(String assetID3Tagged) {
        this.assetID3Tagged = assetID3Tagged;
    }

    public String getAssetInfoUrl() {
        return assetInfoUrl;
    }

    public void setAssetInfoUrl(String assetInfoUrl) {
        this.assetInfoUrl = assetInfoUrl;
    }

    public String getAssetIsLinear() {
        return assetIsLinear;
    }

    public void setAssetIsLinear(String assetIsLinear) {
        this.assetIsLinear = assetIsLinear;
    }

    public String getAssetIsLive() {
        return assetIsLive;
    }

    public void setAssetIsLive(String assetIsLive) {
        this.assetIsLive = assetIsLive;
    }

    public String getAssetProgramGuid() {
        return assetProgramGuid;
    }

    public void setAssetProgramGuid(String assetProgramGuid) {
        this.assetProgramGuid = assetProgramGuid;
    }

    public String getAssetProgramType() {
        return assetProgramType;
    }

    public void setAssetProgramType(String assetProgramType) {
        this.assetProgramType = assetProgramType;
    }

    public String getAssetPublisherAssetID() {
        return assetPublisherAssetID;
    }

    public void setAssetPublisherAssetID(String assetPublisherAssetID) {
        this.assetPublisherAssetID = assetPublisherAssetID;
    }

    public String getAssetRating() {
        return assetRating;
    }

    public void setAssetRating(String assetRating) {
        this.assetRating = assetRating;
    }

    public String getAssetSVOD() {
        return assetSVOD;
    }

    public void setAssetSVOD(String assetSVOD) {
        this.assetSVOD = assetSVOD;
    }

    public String getAssetTitle() {
        return assetTitle;
    }

    public void setAssetTitle(String assetTitle) {
        this.assetTitle = assetTitle;
    }

    public String getAssetURL() {
        return assetURL;
    }

    public void setAssetURL(String assetURL) {
        this.assetURL = assetURL;
    }

    public String getAssetUsingDynamicAds() {
        return assetUsingDynamicAds;
    }

    public void setAssetUsingDynamicAds(String assetUsingDynamicAds) {
        this.assetUsingDynamicAds = assetUsingDynamicAds;
    }

    public Double getBitrate() {
        return bitrate;
    }

    public void setBitrate(Double bitrate) {
        this.bitrate = bitrate;
    }

    public String getCdn() {
        return cdn;
    }

    public void setCdn(String cdn) {
        this.cdn = cdn;
    }

    public Integer getClipCount() {
        return clipCount;
    }

    public void setClipCount(Integer clipCount) {
        this.clipCount = clipCount;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getPlayType() {
        return playType;
    }

    public void setPlayType(String playType) {
        this.playType = playType;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public Long getTotalVideoPlaytime() {
        return totalVideoPlaytime;
    }

    public void setTotalVideoPlaytime(Long totalVideoPlaytime) {
        this.totalVideoPlaytime = totalVideoPlaytime;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getPlatformID() {
        return platformID;
    }

    public void setPlatformID(String platformID) {
        this.platformID = platformID;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getDeviceVendor() {
        return deviceVendor;
    }

    public void setDeviceVendor(String deviceVendor) {
        this.deviceVendor = deviceVendor;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public String getDeviceName() {
        return deviceName;
    }

    public void setDeviceName(String deviceName) {
        this.deviceName = deviceName;
    }

    public String getUniqueDeviceID() {
        return uniqueDeviceID;
    }

    public void setUniqueDeviceID(String uniqueDeviceID) {
        this.uniqueDeviceID = uniqueDeviceID;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getDMA() {
        return DMA;
    }

    public void setDMA(String DMA) {
        this.DMA = DMA;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getISPInformation() {
        return ISPInformation;
    }

    public void setISPInformation(String ISPInformation) {
        this.ISPInformation = ISPInformation;
    }

    public String getIPAddress() {
        return IPAddress;
    }

    public void setIPAddress(String IPAddress) {
        this.IPAddress = IPAddress;
    }

    public String getTimeZoneOffset() {
        return TimeZoneOffset;
    }

    public void setTimeZoneOffset(String timeZoneOffset) {
        TimeZoneOffset = timeZoneOffset;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getOSVersion() {
        return OSVersion;
    }

    public void setOSVersion(String OSVersion) {
        this.OSVersion = OSVersion;
    }

    public String getOSType() {
        return OSType;
    }

    public void setOSType(String OSType) {
        this.OSType = OSType;
    }

    public String getCSLVersion() {
        return CSLVersion;
    }

    public void setCSLVersion(String CSLVersion) {
        this.CSLVersion = CSLVersion;
    }

    public String getAppVersion() {
        return AppVersion;
    }

    public void setAppVersion(String appVersion) {
        AppVersion = appVersion;
    }

    public String getPlayerFrameworkName() {
        return PlayerFrameworkName;
    }

    public void setPlayerFrameworkName(String playerFrameworkName) {
        PlayerFrameworkName = playerFrameworkName;
    }

    public String getPlayerFrameworkVersion() {
        return PlayerFrameworkVersion;
    }

    public void setPlayerFrameworkVersion(String playerFrameworkVersion) {
        PlayerFrameworkVersion = playerFrameworkVersion;
    }

    public Boolean getDebugBuild() {
        return DebugBuild;
    }

    public void setDebugBuild(Boolean debugBuild) {
        DebugBuild = debugBuild;
    }

    public Boolean getSupportMode() {
        return SupportMode;
    }

    public void setSupportMode(Boolean supportMode) {
        SupportMode = supportMode;
    }

    public String getEncryptionType() {
        return encryptionType;
    }

    public void setEncryptionType(String encryptionType) {
        this.encryptionType = encryptionType;
    }

    public String getHashValue() {
        return hashValue;
    }

    public void setHashValue(String hashValue) {
        this.hashValue = hashValue;
    }

    public String getHashType() {
        return hashType;
    }

    public void setHashType(String hashType) {
        this.hashType = hashType;
    }

    public String getxSupportMode() {
        return xSupportMode;
    }

    public void setxSupportMode(String xSupportMode) {
        this.xSupportMode = xSupportMode;
    }

    public String getxDeviceId() {
        return xDeviceId;
    }

    public void setxDeviceId(String xDeviceId) {
        this.xDeviceId = xDeviceId;
    }

    public String getxService() {
        return xService;
    }

    public void setxService(String xService) {
        this.xService = xService;
    }

    public Long getReceivedAtInUtcMs() {
        return receivedAtInUtcMs;
    }

    public void setReceivedAtInUtcMs(Long receivedAtInUtcMs) {
        this.receivedAtInUtcMs = receivedAtInUtcMs;
    }

    public String getOpenrestyRequestId() {
        return openrestyRequestId;
    }

    public void setOpenrestyRequestId(String openrestyRequestId) {
        this.openrestyRequestId = openrestyRequestId;
    }

    public String getRequestLargePayloadAllowed() {
        return requestLargePayloadAllowed;
    }

    public void setRequestLargePayloadAllowed(String requestLargePayloadAllowed) {
        this.requestLargePayloadAllowed = requestLargePayloadAllowed;
    }

    public String getRequestBodySize() {
        return requestBodySize;
    }

    public void setRequestBodySize(String requestBodySize) {
        this.requestBodySize = requestBodySize;
    }

    public String getRemoteClientIpAddress() {
        return remoteClientIpAddress;
    }

    public void setRemoteClientIpAddress(String remoteClientIpAddress) {
        this.remoteClientIpAddress = remoteClientIpAddress;
    }

    public String getRemoteIpAddress() {
        return remoteIpAddress;
    }

    public void setRemoteIpAddress(String remoteIpAddress) {
        this.remoteIpAddress = remoteIpAddress;
    }

    public String getKafkaRequestKey() {
        return kafkaRequestKey;
    }

    public void setKafkaRequestKey(String kafkaRequestKey) {
        this.kafkaRequestKey = kafkaRequestKey;
    }

    public String getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(String receivedAt) {
        this.receivedAt = receivedAt;
    }

    public Long getReceivedAtInUtcSeconds() {
        return receivedAtInUtcSeconds;
    }

    public void setReceivedAtInUtcSeconds(Long receivedAtInUtcSeconds) {
        this.receivedAtInUtcSeconds = receivedAtInUtcSeconds;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getPost_time_secs() {
        return post_time_secs;
    }

    public void setPost_time_secs(Long post_time_secs) {
        this.post_time_secs = post_time_secs;
    }

    public String getPost_time_text() {
        return post_time_text;
    }

    public void setPost_time_text(String post_time_text) {
        this.post_time_text = post_time_text;
    }

    public String getPost_time_date() {
        return post_time_date;
    }

    public void setPost_time_date(String post_time_date) {
        this.post_time_date = post_time_date;
    }

    public Integer getPost_time_hour() {
        return post_time_hour;
    }

    public void setPost_time_hour(Integer post_time_hour) {
        this.post_time_hour = post_time_hour;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getCdnName() {
        return cdnName;
    }

    public void setCdnName(String cdnName) {
        this.cdnName = cdnName;
    }

    public String getContainerId() {
        return containerId;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public String getCmsChannelGuid() {
        return cmsChannelGuid;
    }

    public void setCmsChannelGuid(String cmsChannelGuid) {
        this.cmsChannelGuid = cmsChannelGuid;
    }

    public String getAvailabilityType() {
        return availabilityType;
    }

    public void setAvailabilityType(String availabilityType) {
        this.availabilityType = availabilityType;
    }

    public String getProgramName() {
        return programName;
    }

    public void setProgramName(String programName) {
        this.programName = programName;
    }

    public String getProgramType() {
        return programType;
    }

    public void setProgramType(String programType) {
        this.programType = programType;
    }

    public Long getStartupDuration() {
        return startupDuration;
    }

    public void setStartupDuration(Long startupDuration) {
        this.startupDuration = startupDuration;
    }

    public Long getTotalBufferTime() {
        return totalBufferTime;
    }

    public void setTotalBufferTime(Long totalBufferTime) {
        this.totalBufferTime = totalBufferTime;
    }

    public String getErrorPath() {
        return errorPath;
    }

    public void setErrorPath(String errorPath) {
        this.errorPath = errorPath;
    }

    public String getErrorTypeName() {
        return errorTypeName;
    }

    public void setErrorTypeName(String errorTypeName) {
        this.errorTypeName = errorTypeName;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getClosedType() {
        return closedType;
    }

    public void setClosedType(String closedType) {
        this.closedType = closedType;
    }

    public String getSeverityName() {
        return severityName;
    }

    public void setSeverityName(String severityName) {
        this.severityName = severityName;
    }

    public String getPlayerState() {
        return playerState;
    }

    public void setPlayerState(String playerState) {
        this.playerState = playerState;
    }

    public Boolean getSeekInducedBuffering() {
        return seekInducedBuffering;
    }

    public void setSeekInducedBuffering(Boolean seekInducedBuffering) {
        this.seekInducedBuffering = seekInducedBuffering;
    }

    public Long getFlink_kafka_event_write_ts() {
        return flink_kafka_event_write_ts;
    }

    public void setFlink_kafka_event_write_ts(Long flink_kafka_event_write_ts) {
        this.flink_kafka_event_write_ts = flink_kafka_event_write_ts;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getSeasonNumber() {
        return seasonNumber;
    }

    public void setSeasonNumber(String seasonNumber) {
        this.seasonNumber = seasonNumber;
    }

    public String getCdnHost() {
        return cdnHost;
    }

    public void setCdnHost(String cdnHost) {
        this.cdnHost = cdnHost;
    }

    public String getNetworkType() {
        return networkType;
    }

    public void setNetworkType(String networkType) {
        this.networkType = networkType;
    }

    public String getSubscriptionPacks() {
        return subscriptionPacks;
    }

    public void setSubscriptionPacks(String subscriptionPacks) {
        this.subscriptionPacks = subscriptionPacks;
    }

    public String getDeviceModelYear() {
        return deviceModelYear;
    }

    public void setDeviceModelYear(String deviceModelYear) {
        this.deviceModelYear = deviceModelYear;
    }

    public String getDeviceDisplayMode() {
        return deviceDisplayMode;
    }

    public void setDeviceDisplayMode(String deviceDisplayMode) {
        this.deviceDisplayMode = deviceDisplayMode;
    }

    public String getSoftwareBrowserName() {
        return softwareBrowserName;
    }

    public void setSoftwareBrowserName(String softwareBrowserName) {
        this.softwareBrowserName = softwareBrowserName;
    }

    public String getSoftwareBrowserVersion() {
        return softwareBrowserVersion;
    }

    public void setSoftwareBrowserVersion(String softwareBrowserVersion) {
        this.softwareBrowserVersion = softwareBrowserVersion;
    }

    public Long getWaitTime() {
        return waitTime;
    }

    public void setWaitTime(Long waitTime) {
        this.waitTime = waitTime;
    }

    public Integer getSeverityId() { return severityId; }

    public void setSeverityId(Integer severityId) { this.severityId = severityId; }

    public String getInternalPlayerErrorCode() {
        return internalPlayerErrorCode;
    }

    public void setInternalPlayerErrorCode(String internalPlayerErrorCode) {
        this.internalPlayerErrorCode = internalPlayerErrorCode;
    }

    public String getDrm() {
        return drm;
    }

    public void setDrm(String drm) {
        this.drm = drm;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getChipset() {
        return chipset;
    }

    public void setChipset(String chipset) {
        this.chipset = chipset;
    }

    public String getTunerType() {
        return tunerType;
    }

    public void setTunerType(String tunerType) {
        this.tunerType = tunerType;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public Boolean getOta() {
        return ota;
    }

    public void setOta(Boolean ota) {
        this.ota = ota;
    }

    public String getAdInsertType() {
        return adInsertType;
    }

    public void setAdInsertType(String adInsertType) {
        this.adInsertType = adInsertType;
    }
}
