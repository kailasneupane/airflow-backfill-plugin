document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll("#submissions-list li").forEach(function (item) {
        item.addEventListener("click", function () {
            var logs = item.getAttribute("data-logs");
            document.getElementById("logs-text").innerText = logs;
            document.getElementById("logs-modal").style.display = "flex";
        });
    });

    document.getElementById("logs-modal").addEventListener("click", function (event) {
        if (event.target == document.getElementById("logs-modal")) {
            closeLogs();
        }
    });

    const submissionsList = document.getElementById('submissions-list');
    const submissions = submissionsList.getElementsByTagName('li');

    for (let submission of submissions) {
        const logStatus = submission.querySelector('.log-status');
        const status = submission.getAttribute('data-status');
        switch (status) {
            case '-1':
                logStatus.textContent = 'Waiting for logs';
                logStatus.style.color = '#7d7d7d';
                break;
            case '0':
                logStatus.textContent = 'Failed';
                logStatus.style.color = "#c50000";
                break;
            case '1':
                logStatus.textContent = 'Success';
                logStatus.style.color = "#007bff";

                break;
            default:
                logStatus.textContent = 'Unknown Status';
        }
        logStatus.style.float = 'right';
        logStatus.style.fontWeight = 'bold';
    }
});

function closeLogs() {
    document.getElementById("logs-modal").style.display = "none";
}

function toggleTasksSelection() {
    const tasksContainer = document.getElementById('tasks-container');
    const isChecked = document.getElementById('enable_tasks').checked;
    tasksContainer.style.display = isChecked ? 'block' : 'none';
}

function updateTasks() {
    const dag = document.getElementById('dag').value;
    const tasksSelect = document.getElementById('tasks');

    tasksSelect.innerHTML = '';

    if (dag) {
        const tasks = dagsAndTasks[dag];
        tasks.forEach(task => {
            const option = document.createElement('option');
            option.value = task;
            option.text = task;
            tasksSelect.appendChild(option);
        });
        tasksSelect.size = tasks.length;
    } else {
        tasksSelect.size = 1;
    }
}

function showInfo() {
    document.getElementById("info-modal").style.display = "flex";
}

function closeInfo() {
    document.getElementById("info-modal").style.display = "none";
}

function setDefaultDateTime(event) {
    let now = new Date();
    now.setUTCDate(now.getUTCDate() + 1);
    now.setUTCHours(0, 0, 0, 0);
    const inputId = event.target.id;
    if (document.getElementById(inputId).value === '') {
        document.getElementById(inputId).value = now.toISOString().slice(0, 16);
    }
}