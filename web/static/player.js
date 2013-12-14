// player

var sounds = {};
var lastSound;
var sm = soundManager;

function togglePlay(el) {
	var id = this.id;

	if (lastSound && lastSound != id) {
		console.log('kill last sound ', lastSound);
		toggleButton(lastSound, true);
		sm.stop(lastSound);
		// touch todo (from page-player)
		sm.unload(lastSound);
		delete sounds[lastSound];
	}

	if (!sounds[id]) {
		console.log('create new ', id);
		sounds[id] = soundManager.createSound({id: id, url: $(this).data('url')});
		sounds[id].play();
	} else {
		console.log('exists, toggle pause');
		sounds[id].togglePause();	        		
	}

	lastSound = id;
	toggleButton(id, sounds[id].paused);

	return sounds[id];
}

function toggleButton(id, paused) {
	var el = $('#' + id);
	el.toggleClass('playing', !paused);
	
	var gl = $('#' + id).find('.glyphicon');
	gl.toggleClass('glyphicon-play', paused);
	gl.toggleClass('glyphicon-pause', !paused);
}

$('.playlist .track').on('click', togglePlay);
