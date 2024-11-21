import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';


// Declare global Google API types
declare global {
  interface Window {
    google: {
      accounts: {
        id: {
          initialize: (config: any) => void;
          renderButton: (element: HTMLElement, config: any) => void;
          prompt: () => void;
        };
      };
    };
  }
}

// Google OAuth configuration
const googleConfig = {
  clientId: process.env.REACT_APP_CLIENT_ID
};

interface SignInModalProps {
  isOpen: boolean;
  onClose: () => void;
}

const SignInModal: React.FC<SignInModalProps> = ({ isOpen, onClose }) => {
  const [error, setError] = useState('');
  const navigate = useNavigate();

  useEffect(() => {
    // Load the Google Identity Services SDK
    const loadGoogleScript = () => {
      const existingScript = document.querySelector('script[src="https://accounts.google.com/gsi/client"]');
      if (!existingScript) {
        const script = document.createElement('script');
        script.src = 'https://accounts.google.com/gsi/client';
        script.async = true;
        script.defer = true;
        script.onload = initializeGoogleSignIn;
        document.body.appendChild(script);
      } else {
        initializeGoogleSignIn();
      }
    };

    if (isOpen) {
      loadGoogleScript();
    }
  }, [isOpen]);

  const initializeGoogleSignIn = () => {
    if (window.google?.accounts?.id) {
      window.google.accounts.id.initialize({
        client_id: googleConfig.clientId,
        callback: handleGoogleCredentialResponse,
      });

      // Render the Google Sign In button
      const buttonContainer = document.getElementById('googleSignInDiv');
      if (buttonContainer) {
        window.google.accounts.id.renderButton(buttonContainer, {
          type: 'standard',
          theme: 'outline',
          size: 'large',
          text: 'continue_with',
          width: '100%',
        });
      }
    }
  };

  const handleGoogleCredentialResponse = (response: any) => {
    try {
      const { credential } = response;

      // Store the token locally
      localStorage.setItem('googleToken', credential);

      // Close the modal upon successful login
      onClose();
      navigate('/dashboard-home');
    } catch (error) {
      setError('Failed to sign in with Google');
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex justify-center items-center">
      <div className="bg-white p-8 rounded-lg shadow-xl w-96">
        <h2 className="text-2xl font-bold mb-6 text-center">Sign In</h2>
        {error && (
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative mb-4">
            {error}
          </div>
        )}

        {/* Google Sign In Button */}
        <div id="googleSignInDiv" className="w-full mb-4"></div>
      </div>
    </div>
  );
};

export default SignInModal;


  // import React, { useState } from 'react';
  // import { useNavigate } from 'react-router-dom';
  
  // // Define an interface for the sign-in result to improve type safety
  // interface SignInResult {
  //   success: boolean;
  //   token?: string;
  //   message?: string;
  // }
  
  // const SignInModal = ({ isOpen, onClose }: { isOpen: boolean; onClose: () => void }) => {
  //   const [email, setEmail] = useState<string>('');
  //   const [password, setPassword] = useState<string>('');
  //   const [error, setError] = useState<string>('');
  //   const navigate = useNavigate();
  
  //   // Mock sign-in function with typed parameters and return type
  //   const mockSignIn = (email: string, password: string): SignInResult => {
  //     const validEmail = 'john.doe@gmail.com';
  //     const validPassword = 'pass';
      
  //     if (email === validEmail && password === validPassword) {
  //       return { success: true, token: 'mock-jwt-token' };
  //     }
  //     return { success: false, message: 'Invalid email or password' };
  //   };
  
  //   const handleSignIn = (e: React.FormEvent) => {
  //     e.preventDefault();
  //     setError('');
  
  //     const result = mockSignIn(email, password);
  
  //     if (result.success && result.token) {
  //       // Safely store token only if it exists
  //       localStorage.setItem('accessToken', result.token);
  
  //       // Close modal and redirect to dashboard
  //       onClose();
  //       navigate('/dashboard-home');
  //     } else {
  //       // Use the message if available, or a default error message
  //       setError(result.message || 'Sign-in failed');
  //     }
  //   };
  
  //   if (!isOpen) return null;
  
  //   return (
  //     <div className="fixed inset-0 bg-black bg-opacity-50 flex justify-center items-center">
  //       <div className="bg-white p-8 rounded-lg shadow-xl w-96">
  //         <h2 className="text-2xl font-bold mb-6 text-center">Sign In</h2>
  //         {error && (
  //           <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative mb-4">
  //             {error}
  //           </div>
  //         )}
  //         <form onSubmit={handleSignIn}>
  //           <div className="mb-4">
  //             <label htmlFor="email" className="block text-gray-700 mb-2">Email</label>
  //             <input
  //               type="email"
  //               id="email"
  //               value={email}
  //               onChange={(e) => setEmail(e.target.value)}
  //               className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
  //               required
  //             />
  //           </div>
  //           <div className="mb-6">
  //             <label htmlFor="password" className="block text-gray-700 mb-2">Password</label>
  //             <input
  //               type="password"
  //               id="password"
  //               value={password}
  //               onChange={(e) => setPassword(e.target.value)}
  //               className="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
  //               required
  //             />
  //           </div>
  //           <button
  //             type="submit"
  //             className="w-full bg-blue-600 text-white py-2 rounded-lg hover:bg-blue-700 transition duration-300"
  //           >
  //             Sign In
  //           </button>
  //         </form>
  //         <div className="mt-4 text-center">
  //           <a href="#forgot-password" className="text-blue-600 hover:underline">
  //             Forgot Password?
  //           </a>
  //         </div>
  //       </div>
  //     </div>
  //   );
  // };
  
  // export default SignInModal;