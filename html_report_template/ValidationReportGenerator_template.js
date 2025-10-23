function showTab(tabId) {
    document.getElementById('summaryTab').style.display = 'none';
    document.getElementById('schemaTab').style.display = 'none';
    document.getElementById('rowCountTab').style.display = 'none';
    document.getElementById('dataTab').style.display = 'none';
    document.getElementById('dataTabDetail').style.display = 'none';
    document.getElementById('ruleBasedValidation').style.display = 'none';
    document.getElementById('distributionBasedValidation').style.display = 'none';
    document.getElementById(tabId).style.display = 'block';
    var buttons = document.getElementsByClassName('tab-button');

    for (var i = 0; i < buttons.length; i++) {
        buttons[i].classList.remove('active-tab');
    }

    if (tabId === 'summaryTab') {
        buttons[0].classList.add('active-tab');
    } else if (tabId === 'schemaTab') {
        buttons[1].classList.add('active-tab');
    } else if (tabId === 'rowCountTab') {
        buttons[2].classList.add('active-tab');
    } else if (tabId === 'dataTab') {
        buttons[3].classList.add('active-tab');
    } else if (tabId === 'dataTabDetail') {
        buttons[4].classList.add('active-tab');
    } else if (tabId === 'ruleBasedValidation') {
        buttons[5].classList.add('active-tab');
    } else if (tabId === 'distributionBasedValidation') {
        buttons[6].classList.add('active-tab');
    } else {
        buttons[0].classList.add('active-tab');
    }
}

function showGroupTab(tabId) {
    var buttons = document.getElementsByClassName('group-tab-button');

    if (buttons.length === 0) {
        document.getElementById('groupTab_1').style.display = 'block';
        return;
    }

    for (var i = 0; i < buttons.length; i++) {
        buttons[i].classList.remove('active-tab');
        document.getElementById('groupTab_' + (i + 1)).style.display = 'none';

        if (tabId === 'groupTab_' + (i + 1)) {
            buttons[i].classList.add('active-tab');
        }
    }

    document.getElementById(tabId).style.display = 'block';
}


window.addEventListener('DOMContentLoaded', function () {
    showTab('summaryTab');
    showGroupTab('groupTab_1');

    // In the Rule-Based Data Validation section, highlight patterns in descriptions of the Successful Samples.
    // Example: START_POSITION matched the positive integer without leading zeros, up to 11 digits. But optionally allows a .0 at the end.
    // So the text "Optionally allows a .0 at the end" will be highlighted. to indicate special attention. Many values with a .0 suffix (e.g., 123.0). These should be converted to integers before any manual comparison or calculation for accurate results. When time allows, it is recommended to standardize these fields as integer values.

    // Separator used to mark highlighted text in descriptions
    const highlight_separator = "!!!";

    // Get all elements that may contain highlighted patterns
    const descriptionps = document.getElementsByClassName("success-samples-pattern-regex-description");

    // Build a regex to find text between the highlight separators
    const regex = new RegExp(highlight_separator + "(.*?)" + highlight_separator, "g");

    // Replace marked text with a span for highlighting
    for (let i = 0; i < descriptionps.length; i++) {
        descriptionps[i].innerHTML = descriptionps[i].textContent.replace(regex, '<span class="success-samples-pattern-regex-description-highlight">$1</span>');
    }
});

// Scroll to top button logic
window.addEventListener('DOMContentLoaded', function () {
    var btn = document.getElementById('scrollToTopBtn');
    window.addEventListener('scroll', function () {
        // Show button if scrolled more than 1 viewport height
        if (window.scrollY > window.innerHeight - 400) {
            btn.style.display = 'block';
        } else {
            btn.style.display = 'none';
        }
    });
    btn.addEventListener('click', function () {
        window.scrollTo({ top: 0, behavior: 'smooth' });
    });
});


