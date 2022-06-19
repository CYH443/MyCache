package com.cachekit.core.control.event;

public enum ElementEventType {
    EXCEEDED_MAXLIFE_BACKGROUND, //缓存过期时长

    EXCEEDED_MAXLIFE_ONREQUEST,

    EXCEEDED_IDLETIME_BACKGROUND, //缓存闲置时长

    EXCEEDED_IDLETIME_ONREQUEST,

    SPOOLED_DISK_AVAILABLE, //刷盘：磁盘可用

    SPOOLED_DISK_NOT_AVAILABLE, //刷盘：磁盘不可用

    SPOOLED_NOT_ALLOWED //不可刷盘
}
