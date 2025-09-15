document.addEventListener("DOMContentLoaded", () => {
  const queryInput = document.getElementById("query-input");
  const submitButton = document.getElementById("submit-button");
  const chatContainer = document.getElementById("chat-container");
  const loader = document.getElementById("loader");

  const API_URL = "http://127.0.0.1:8000/agent/query";

  let chatHistory = []; // This will now hold our full conversation

  const handleQuery = async () => {
    const query = queryInput.value.trim();
    if (!query) return;

    displayUserMessage(query);
    queryInput.value = ""; // Clear input immediately
    loader.style.display = "block";
    submitButton.disabled = true;

    try {
      const response = await fetch(API_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: query, chat_history: chatHistory }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || "An API error occurred.");
      }

      const data = await response.json();
      chatHistory = data.response.updated_history || []; // Update our history
      displayAgentResponse(data.response);
    } catch (error) {
      console.error("Error fetching data:", error);
      displayError(error.message);
    } finally {
      loader.style.display = "none";
      submitButton.disabled = false;
      queryInput.focus();
    }
  };

  const displayUserMessage = (message) => {
    const messageDiv = document.createElement("div");
    messageDiv.className = "chat-message user-message";
    messageDiv.textContent = message;
    chatContainer.appendChild(messageDiv);
    scrollToBottom();
  };

  const displayAgentResponse = (response) => {
    const messageDiv = document.createElement("div");
    messageDiv.className = "chat-message agent-message";

    if (response.conversational_response) {
      const textP = document.createElement("p");
      textP.textContent = response.conversational_response;
      messageDiv.appendChild(textP);
    }

    if (response.data && response.data.length > 0) {
      const cardDiv = document.createElement("div");
      cardDiv.className = "data-card";
      cardDiv.textContent = JSON.stringify(response.data, null, 2);
      messageDiv.appendChild(cardDiv);
    }

    chatContainer.appendChild(messageDiv);
    scrollToBottom();
  };

  const displayError = (message) => {
    const errorDiv = document.createElement("div");
    errorDiv.className = "chat-message agent-message";
    errorDiv.innerHTML = `<p class="error-message">Error: ${message}</p>`;
    chatContainer.appendChild(errorDiv);
    scrollToBottom();
  };

  const scrollToBottom = () => {
    chatContainer.scrollTop = chatContainer.scrollHeight;
  };

  submitButton.addEventListener("click", handleQuery);
  queryInput.addEventListener("keyup", (event) => {
    if (event.key === "Enter") {
      handleQuery();
    }
  });
});
