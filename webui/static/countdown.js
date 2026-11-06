/*!
 * countdown.js — 端口倒计时关闭（index.html / phone.html 共享）
 *
 * 设备侧：PIID 9~12 是 uint16 分钟，0 = 未设置。界面按"小时 + 分钟"输入和显示，
 * 上限与后端 PIID_RANGES 一致（1440 分钟 = 24 小时）。
 *
 * 依赖：I18N（locale 运行时）。
 */
(function (global) {
    'use strict';

    var PIIDS = { c1: 9, c2: 10, c3: 11, a: 12 };
    var MAX_MINUTES = 1440;
    var QUICK_MINUTES = [30, 60, 120, 240, 480, 720];

    function t(key, params) {
        return (global.I18N && global.I18N.t) ? global.I18N.t(key, params) : key;
    }

    function format(minutes) {
        var total = Math.max(0, Math.round(Number(minutes) || 0));
        if (!total) return t('common.notSet');
        var h = Math.floor(total / 60);
        var m = total % 60;
        if (!h) return t('common.minutes', { count: m });
        if (!m) return t('common.hours', { count: h });
        return t('common.hoursMinutes', { h: h, m: m });
    }

    function quickLabel(minutes) {
        return minutes % 60 === 0
            ? t('countdown.quickHours', { count: minutes / 60 })
            : t('countdown.quick', { count: minutes });
    }

    // "时 + 分"输入 → 分钟数，越界按上限截断（截断后的值会立刻出现在状态文字里）
    function toMinutes(hours, minutes) {
        var h = Math.max(0, parseInt(hours, 10) || 0);
        var m = Math.max(0, parseInt(minutes, 10) || 0);
        return Math.min(h * 60 + m, MAX_MINUTES);
    }

    global.Countdown = {
        PIIDS: PIIDS,
        MAX_MINUTES: MAX_MINUTES,
        QUICK_MINUTES: QUICK_MINUTES,
        format: format,
        quickLabel: quickLabel,
        toMinutes: toMinutes
    };
})(typeof window !== 'undefined' ? window : this);
