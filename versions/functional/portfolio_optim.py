import numpy as np
import cvxpy as cp

# Define the expected returns and covariance matrix of assets
mu = np.array([0.1, 0.2, 0.15])
Sigma = np.array([[0.05, 0.02, 0.01], [0.02, 0.09, 0.05], [0.01, 0.05, 0.08]])

# Define the number of assets and the maximum weight of each asset in the portfolio
n = len(mu)
w_max = 0.2

# Define the optimization problem
w = cp.Variable(n)
ret = mu @ w
risk = cp.quad_form(w, Sigma)
constraints = [cp.sum(w) == 1, w >= 0, w <= w_max]
obj = cp.Maximize(ret - 0.5 * risk)
prob = cp.Problem(obj, constraints)

# Solve the optimization problem
prob.solve()

# Print the optimal portfolio
print("Optimal portfolio:")
for i in range(n):
    print(f"Asset {i+1}: {w.value[i]:.3f}")
print(f"Expected return: {ret.value:.3f}")
print(f"Risk: {risk.value:.3f}")
