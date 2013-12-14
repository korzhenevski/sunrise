/* ============================================
 * bootstrap-infiniteScroll.js
 * ============================================ */
 
!function ($) {
    'use strict';
    var InfiniteScroll = function (el, options) {
        this.$element = $(el);
        this.$data = $(el).data();
        this.$options = options;
 
        this.executing = false;
        this.endOfResults = false;
        this.currentPage = 1;
 
        var that = this;
 
        $(window).scroll(function () {
            if ($(window).scrollTop() >= that.$options.calculateBottom()) {
                console.log('load more');
                that.loadMore();
            }
        });
    };
 
    InfiniteScroll.prototype = {
        constructor: InfiniteScroll,
 
        loadMore: function () {
            var $this = this;
            if ($this.executing || $this.endOfResults) return;
 
            $this.$element.find('.spinner').removeClass('hide');
 
            $this.executing = true;
            $this.currentPage += 1;
 
            var data = $this.$options.getData();
            data.page = $this.currentPage;
 
            $.ajax({
                data: data,
                url: $this.$options.url,
                success: function (retVal) {
                    $this.$options.processResults(retVal);
 
                    if (retVal.length == 0) {
                        $this.endOfResults = true;
                        $this.$element.find('#end-of-results').removeClass('hide');
                    }
 
                    $this.$element.find('.spinner').addClass('hide');
                    $this.executing = false;
                }
            });
        }
    };
 
    $.fn.infiniteScroll = function (option) {
        return this.each(function () {
            var $this = $(this),
                data = $this.data('infinite-search'),
                options = $.extend({}, $.fn.infiniteScroll.defaults, typeof option == 'object' && option);
            if (!data) $this.data('infinite-search', (data = new InfiniteScroll(this, options)));
            if (typeof options == 'string') data[options]();
        });
    };
 
    $.fn.infiniteScroll.defaults = {
        calculateBottom: function () { },
        getData: function () { },
        processResults: function () { },
        url: ''
    };
 
    $.fn.infiniteScroll.Constructor = InfiniteScroll;
} (window.jQuery);