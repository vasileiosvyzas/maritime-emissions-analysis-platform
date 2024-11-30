import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';

interface APIAccessRequestProps {
    userEmail: string;
  }

const APIAccessRequest: React.FC<APIAccessRequestProps> = ({userEmail: initialEmail}) => {
  const [purpose, setPurpose] = useState('');
  const [userEmail, setUserEmail] = useState(initialEmail);
  const [submitted, setSubmitted] = useState(false);
  const navigate = useNavigate();

  const handleReturnToDashboard = () => {
    // Navigate to API request page or open a modal for requesting access
    window.location.href = '/dashboard-home';
  };

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    
    try {
      const response = await fetch('/api/request-api-key', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          email: userEmail,
          purpose: purpose
        })
      });

      if (response.ok) {
        setSubmitted(true);
      }
    } catch (error) {
      console.error('API key request failed', error);
    }
  };

  if (submitted) {
    return (
      <div className="max-w-md mx-auto p-6 bg-white rounded-lg shadow-md">
        <h2 className="text-2xl font-bold mb-4">API Access Request Submitted</h2>
        <p className="mb-4">
          We've received your request. You'll be notified via email about your API key status.
        </p>
        <button 
          onClick={(handleReturnToDashboard)}
          className="w-full bg-blue-500 text-white py-2 rounded hover:bg-blue-600"
        >
          Return to Dashboard
        </button>
      </div>
    );
  }

  return (
    <div className="max-w-md mx-auto p-6 bg-white rounded-lg shadow-md">
      <h2 className="text-2xl font-bold mb-4">Request API Access</h2>
      <form onSubmit={handleSubmit}>
        <div className="mb-4">
          <label htmlFor="email" className="block mb-2 font-medium">
            Email
          </label>
          <input 
            type="email" 
            id="email" 
            value={userEmail} 
            onChange={(e) => setUserEmail(e.target.value)} 
            className="w-full p-2 border rounded bg-gray-100"
          />
        </div>

        <div className="mb-4">
          <label htmlFor="purpose" className="block mb-2 font-medium">
            Purpose of API Usage
          </label>
          <textarea 
            id="purpose"
            value={purpose}
            onChange={(e) => setPurpose(e.target.value)}
            required
            placeholder="Briefly describe how you plan to use the API"
            className="w-full p-2 border rounded"
            // rows="4"
          />
        </div>

        <button 
          type="submit" 
          className="w-full bg-blue-500 text-white py-2 rounded hover:bg-blue-600"
        >
          Submit API Access Request
        </button>
      </form>
    </div>
  );
};

export default APIAccessRequest;