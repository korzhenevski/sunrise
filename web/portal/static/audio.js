soundManager.setup({url: '/static/', preferFlash: true, debugMode: false});

angular.module('afm', [])

.factory('sound', function($rootScope){
  var sound;
  var obj = {
    load: function(url) {
      if (sound) {
        sound.destruct();
      }      
      sound = soundManager.createSound({
        id: 'stream',
        url: url,
        onplay: function() {
          $rootScope.$broadcast('sound:play');
        }
      });
    },

    paused: function() {
      if (sound) {
        return sound.playState === 0;
      }
      return true;
    },

    play: function() {
      console.log('play', sound);
      if (sound) {
        sound.play();        
      }
    },

    stop: function() {
      console.log('stop', sound);
      if (sound) {
        sound.stop();
        sound.unload();
      }
    }
  };

  return obj;
})

.controller('PlayerCtrl', function($scope, sound){
  $scope.player = {};
  $scope.select = function(info) {
    $scope.player = info;
    $scope.switchBitrate(info.bitrate[0]);
  };

  $scope.switchBitrate = function(bitrate){
    $scope.bitrate = bitrate;
    sound.load($scope.player.url + '&bitrate=' + $scope.bitrate);
    sound.play();
  };

  $scope.play = function() {
    sound.play();
  };
  
  $scope.stop = function() {
    sound.stop();
  }
  $scope.isPaused = function() {
    return sound.paused();
  }
});

