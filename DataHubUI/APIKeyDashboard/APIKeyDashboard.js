// --------------------------------------------------
//  Constants
// --------------------------------------------------
/**
 * Fields define the modal form structure.
 * Each field includes an ID, label, input type, placeholder text, requirement flag, and optional button for auto-generation.
 */
const fields = [
    { id: 'modal-access-key', label: 'Access Key', type: 'text', placeholder: 'Enter access key', required: true, button: true, columnIndex: 4 },
    { id: 'modal-secret-key', label: 'Secret Key', type: 'text', placeholder: 'Enter secret key', required: true, button: true, columnIndex: 5 },
    { id: 'modal-key-group', label: 'Key Group', type: 'text', placeholder: 'Enter group', required: true, defaultValue: 'Default', columnIndex: 0 }, // Added defaultValue
    { id: 'modal-note', label: 'Note', type: 'text', placeholder: 'Enter note (optional)', required: false, columnIndex: 3 },
    { id: 'modal-start-date', label: 'Start Date UTC', type: 'date', placeholder: '', required: false, columnIndex: 1 },
    { id: 'modal-end-date', label: 'End Date UTC', type: 'date', placeholder: '', required: false, columnIndex: 2 }
];


// --------------------------------------------------
//  Event Handlers
// --------------------------------------------------
/**
 * Handles the deletion of a key with a confirmation dialog.
 * Calls the Python function 'delete_key' with the access key as an argument if confirmed.
 *
 * @param {HTMLButtonElement} button - Button element triggering the delete action.
 */
function onDeleteKey(button) {
    const row = button.closest('tr');
    const accessKey = row.cells[4].textContent;

    // Show confirmation dialog
    const confirmed = confirm(`Are you sure you want to delete the key with Access Key: "${accessKey}"?`);
    if (!confirmed) {
        console.log(`Deletion of key "${accessKey}" was canceled.`);
        return;
    }

    // Call the Python function to delete the key
    call_py('delete_key', accessKey);
}

/**
 * Opens the modal dialog for adding or editing a key.
 * Resets all modal fields and populates them with data from the provided table row, if available.
 *
 * @param {HTMLTableRowElement|null} row - The table row containing data to populate the modal fields.
 *                                         If `null`, the modal opens with empty fields for a new entry.
 */
function onOpenModal(row = null) {
    if (!window.bootstrapModalInstance) {
        console.error("Bootstrap modal instance is not initialized.");
        return;
    }

    // Clear and reset all modal fields
    fields.forEach(({ id, defaultValue }) => {
        const field = document.getElementById(id);
        if (field) {
            field.value = defaultValue || ''; // Reset field to defaultValue if defined, else empty
        } else {
            console.warn(`Field with ID "${id}" not found.`);
        }
    });

    if (row) {
        const cells = row.querySelectorAll('td');
        if (!cells.length) {
            console.warn("No cells found in the row to populate the modal.");
            return;
        }

        // Populate modal fields based on the columnIndex in the fields array
        fields.forEach(({ id, columnIndex }) => {
            const cell = cells[columnIndex];
            const field = document.getElementById(id);
            if (cell && field) {
                field.value = cell.textContent.trim(); // Assign cell content to the field
                console.log(`Assigning value "${cell.textContent.trim()}" to field "${id}"`);
            } else if (!field) {
                console.warn(`Field with ID "${id}" not found.`);
            } else {
                console.warn(`No cell found for column index ${columnIndex}.`);
            }
        });
    }

    window.bootstrapModalInstance.show();
}


/**
 * Saves a key by collecting modal input values and calling the 'save_key' Python function.
 * Clears modal inputs after saving and hides the modal.
 *
 * @param {Event} event - Submit event of the form.
 */
function onSaveKey(event) {
    event.preventDefault();

    const formData = fields.reduce((data, { id }) => {
        data[id] = document.getElementById(id).value;
        return data;
    }, {});

    const { 'modal-key-group': keyGroup, 'modal-start-date': startDate, 'modal-end-date': endDate, 'modal-note': note, 'modal-access-key': accessKey, 'modal-secret-key': secretKey } = formData;

    call_py('save_key', keyGroup, startDate, endDate, note, accessKey, secretKey);
    bootstrap.Modal.getInstance(document.getElementById('keyModal')).hide();

    fields.forEach(({ id }) => {
        const field = document.getElementById(id);
        if (field) field.value = '';
    });
}

// --------------------------------------------------
//  Functions
// --------------------------------------------------
/**
 * Initializes the modal by populating its body with input fields.
 * This function is called when the document is ready.
 */
document.addEventListener('DOMContentLoaded', () => {
    const modalBody = document.getElementById('keyModalBody');
    if (!modalBody) {
        console.error("Modal body element with ID 'keyModalBody' not found.");
        return;
    }

    // Populate the modal body with fields dynamically
    modalBody.innerHTML = fields.map(({ id, label, type, placeholder, required, button, defaultValue }) => `
        <div class="mb-3">
            <label for="${id}" class="form-label">${label}</label>
            <div class="input-group">
                <input type="${type}" id="${id}" class="form-control" placeholder="${placeholder}" ${required ? 'required' : ''} value="${defaultValue || ''}">
                ${button ? `<button type="button" class="btn btn-outline-primary" onclick="call_py('generate_key', '${id}')">Auto-generate</button>` : ''}
            </div>
        </div>
    `).join('');

    // Initialize the Bootstrap modal instance and store it globally for reuse
    window.bootstrapModalInstance = new bootstrap.Modal(document.getElementById('keyModal'));
});


/**
 * Adds or updates a table row based on key data.
 *
 * @param {Object} key - Key data object.
 * @param {string} key.key_group - Key Group.
 * @param {string} key.start_date_UTC - Start date (YYYY-MM-DD).
 * @param {string} key.end_date_UTC - End date (YYYY-MM-DD).
 * @param {string} key.note - Note or description.
 * @param {string} key.access_key - Access key.
 * @param {string} key.secret_key - Secret key.
 * @param {string} key.status - Key status.
 */
function createOrUpdateTableRow(key) {
    const keyTableBody = document.getElementById('key-table-body');
    const rows = [...keyTableBody.rows];
    const existingRowIndex = rows.findIndex(row => row.cells[4].textContent === key.access_key);

    const row = document.createElement('tr');
    row.innerHTML = `
        <td>${key.key_group}</td>
        <td>${key.start_date_UTC}</td>
        <td>${key.end_date_UTC}</td>
        <td>${key.note}</td>
        <td>${key.access_key}</td>
        <td>${key.secret_key}</td>
        <td>${key.status}</td>
        <td class="actions">
            <button class="btn btn-outline-warning btn-sm" onclick="onOpenModal(this.closest('tr'))">Edit</button>
            <button class="btn btn-outline-secondary btn-sm" onclick="call_py('toggle_suspend_key', '${key.access_key}')">Suspend</button>
            <button class="btn btn-outline-danger btn-sm" onclick="onDeleteKey(this)">Delete</button>
        </td>
    `;

    if (existingRowIndex !== -1) {
        keyTableBody.replaceChild(row, rows[existingRowIndex]);
    } else {
        keyTableBody.appendChild(row);
    }
}

/**
 * Deletes a table row based on the provided access key.
 *
 * @param {string} accessKey - The access key of the row to delete.
 */
function deleteTableRow(accessKey) {
    // Get the table body element dynamically
    const keyTableBody = document.getElementById('key-table-body');
    if (!keyTableBody) {
        console.error("Element with ID 'key-table-body' not found.");
        return;
    }

    // Locate the row with the matching accessKey
    const rows = [...keyTableBody.rows];
    const rowIndex = rows.findIndex(row => row.cells[4].textContent === accessKey);

    if (rowIndex !== -1) {
        // Remove the identified row
        keyTableBody.deleteRow(rowIndex);
        console.log(`Row with accessKey "${accessKey}" has been successfully deleted.`);
    } else {
        console.warn(`No row found with accessKey "${accessKey}".`);
    }
}
